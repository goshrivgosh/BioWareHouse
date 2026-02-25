import os
import logging
import tempfile
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from minio import Minio
import configparser
import pysam
from howard.tools.convert import convert
from howard.tools.tools import arguments_dict
import argparse


current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'configs', 'connections.ini')
config = configparser.ConfigParser()
config.read(config_path)

SERVER = 'minio'
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = config[SERVER]['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = config[SERVER]['MINIO_SECRET_KEY']

SOURCE_BUCKET = 'raw-vcf-files'
SOURCE_FILE = 'ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz'
DEST_BUCKET = 'sandbox'
CHROMOSOME = '_'.join(SOURCE_FILE.split('.')[:2])

HOWARD_CONFIG = {
    "threads": 4,
    "explode_infos": True,
    "explode_infos_prefix": "",
    "explode_infos_fields": "DP,AF,AC,AN,NS",
    "include_header": True
}

BATCH_SIZE = 20_000


SHARED_TMP = "/opt/airflow/tmp"
os.makedirs(SHARED_TMP, exist_ok=True)


def convert_vcf_to_parquet_with_howard(input_vcf, output_parquet, config):

    args = argparse.Namespace(
        input=input_vcf,
        output=output_parquet,
        config=config,
        explode_infos=config['explode_infos'],
        explode_infos_prefix=config['explode_infos_prefix'],
        explode_infos_fields=config['explode_infos_fields'],
        include_header=config['include_header'],
        arguments_dict=arguments_dict,
    )
    convert(args)


@task
def create_work_dir():

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    work_dir = os.path.join(SHARED_TMP, run_id)
    os.makedirs(work_dir, exist_ok=True)
    logging.info(f"Создана рабочая директория: {work_dir}")
    return work_dir

@task
def split_vcf_to_batches(work_dir: str):

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


    input_path = os.path.join(work_dir, "input.vcf.gz")
    logging.info(f"Скачивание {SOURCE_BUCKET}/{SOURCE_FILE} -> {input_path}")
    obj = client.get_object(SOURCE_BUCKET, SOURCE_FILE)
    with open(input_path, 'wb') as f:
        shutil.copyfileobj(obj, f, length=10 * 1024 * 1024)
    obj.close()
    obj.release_conn()


    vcf_in = pysam.VariantFile(input_path)
    header = vcf_in.header
    total_records = sum(1 for _ in vcf_in)  # подсчёт записей
    vcf_in.close()

    logging.info(f"Всего записей: {total_records}, размер батча: {BATCH_SIZE}")
    num_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
    batch_info_list = []


    batch_idx = 0
    batch_path = os.path.join(work_dir, f"batch_{batch_idx:04d}.vcf.gz")
    vcf_out = pysam.VariantFile(batch_path, 'w', header=header)
    records_written = 0

    with pysam.VariantFile(input_path) as vcf_in:
        for record in vcf_in:
            vcf_out.write(record)
            records_written += 1

            if records_written % BATCH_SIZE == 0:
                vcf_out.close()
                batch_info_list.append({
                    "batch_index": batch_idx,
                    "vcf_path": batch_path,
                })
                batch_idx += 1
                batch_path = os.path.join(work_dir, f"batch_{batch_idx:04d}.vcf.gz")
                vcf_out = pysam.VariantFile(batch_path, 'w', header=header)


    if records_written % BATCH_SIZE != 0:
        vcf_out.close()
        batch_info_list.append({
            "batch_index": batch_idx,
            "vcf_path": batch_path,
        })
    else:
        vcf_out.close()

    logging.info(f"Создано {len(batch_info_list)} батчей")
    return batch_info_list

@task
def convert_batch(batch_info: dict):

    vcf_path = batch_info["vcf_path"]
    batch_idx = batch_info["batch_index"]


    parquet_path = vcf_path.replace('.vcf.gz', '.parquet')
    logging.info(f"Конвертация батча {batch_idx}: {vcf_path} -> {parquet_path}")

    convert_vcf_to_parquet_with_howard(vcf_path, parquet_path, HOWARD_CONFIG)


    dest_object = f"{CHROMOSOME}/batch_{batch_idx:04d}.parquet"
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    with open(parquet_path, 'rb') as f:
        client.put_object(
            bucket_name=DEST_BUCKET,
            object_name=dest_object,
            data=f,
            length=os.path.getsize(parquet_path)
        )

    logging.info(f"Батч {batch_idx} обработан и загружен как {dest_object}")
    return batch_idx

@task
def cleanup(work_dir: str):

    shutil.rmtree(work_dir, ignore_errors=True)
    logging.info(f"Временная папка {work_dir} удалена.")

default_args = {
    'owner': 'sazykin_ga',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='vcf_to_parquet',
    default_args=default_args,
    description='Параллельная конвертация VCF в Parquet с помощью HOWARD',
    start_date=datetime(2026, 2, 19),
    schedule_interval=None,
    catchup=False,
    tags=['minio', 'howard', 'vcf', 'parquet']
) as dag:

    work_dir = create_work_dir()
    batch_info_list = split_vcf_to_batches(work_dir)
    convert_tasks = convert_batch.expand(batch_info=batch_info_list)
    cleanup_task = cleanup(work_dir)


    work_dir >> batch_info_list >> convert_tasks >> cleanup_task