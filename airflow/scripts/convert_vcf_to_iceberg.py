import argparse
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser(description='Конвертирование VCF в Iceberg в MinIO')
    parser.add_argument('--source', required=True, help='S3 path to source VCF file')
    parser.add_argument('--table', required=True, help='Iceberg table name (e.g., local.db.variants_chr22)')
    args = parser.parse_args()

    # SparkSession уже создана с нужными конфигами (ключи, endpoint и т.д.) через spark-submit
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Чтение VCF из {args.source} ...")
    vcf_df = spark.read.format("vcf") \
        .option("flattenInfoFields", "false") \
        .option("includeSampleIds", "true") \
        .load(args.source)

    print(f"Всего вариантов:  {vcf_df.count()}")
    vcf_df.printSchema()

    print(f"Запись в Iceberg {args.table} ...")
    vcf_df.writeTo(args.table) \
        .using("iceberg") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "zstd") \
        .createOrReplace()

    print("Таблица Iceberg создана")

    spark.table(args.table).select("contigName", "start", "referenceAllele", "alternateAlleles").show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()