import os
from pyspark.sql import SparkSession

warehouse_path = os.path.abspath("./iceberg_warehouse")

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.projectglow:glow-spark3_2.12:1.2.1,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1 pyspark-shell'

# Настройки Spark - сессии
spark = SparkSession.builder \
    .appName("Local VCF to Iceberg") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "io.projectglow.sql.GlowSparkExtension") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_path) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
vcf_path = "vcf_files/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
print(f"Чтение VCF из {vcf_path} ...")
vcf_df = spark.read.format("vcf") \
    .option("flattenInfoFields", "false") \
    .option("includeSampleIds", "true") \
    .load(vcf_path)

# Выводим схему - согласно этой схеме будут храниться данные в iceberg - таблице
vcf_df.printSchema()
table_name = "local.db.all_chr9"
print(f"Запись в Iceberg {table_name} (local warehouse: {warehouse_path}) ...")

vcf_df.writeTo(table_name) \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "zstd") \
    .createOrReplace()
print("Таблица Iceberg создана")
print("Пример данных:")
spark.table(table_name).select("contigName", "start", "referenceAllele", "alternateAlleles").show(5, truncate=False)
spark.stop()