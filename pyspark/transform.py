from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType


BUCKET_NAME = "kafka_syce_gcp"
BUSINESS_DOMAIN = "datagov"
SOURCE_FOLDER = f"{BUSINESS_DOMAIN}/raw"
DESTINATION_FOLDER = f"{BUSINESS_DOMAIN}/processed"
KEYFILE_PATH = "/opt/Key/datath-th-kafka-5dbcfce64148.json"

spark = SparkSession.builder.appName("datagov") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

struct_schema = StructType([
    StructField("id", StringType()),
    StructField("รูปแบบการเดินทาง", StringType()),
    StructField("วัตถุประสงค์", StringType()),
    StructField("สาธารณะ/ส่วนบุคคล", StringType()),
    StructField("หน่วยงาน", StringType()),
    StructField("ยานพาหนะ", StringType()),
    StructField("วันที่", TimestampType()),
    StructField("หน่วย", StringType()),
    StructField("ปริมาณ", StringType()),
])

GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{SOURCE_FOLDER}/*.json"

# df = spark.read \
#     .option("inferSchema", True) \
#     .json(GCS_FILE_PATH)

df = spark.read \
    .schema(struct_schema) \
    .json(GCS_FILE_PATH)

df.show()
df.printSchema()

df.createOrReplaceTempView("datagov")
result = spark.sql("""
    select
        *

    from datagov
""")

OUTPUT_PATH = f"gs://{BUCKET_NAME}/{DESTINATION_FOLDER}"
result.write.mode("overwrite").parquet(OUTPUT_PATH)
