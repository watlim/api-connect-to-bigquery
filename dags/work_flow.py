from airflow.decorators import dag,task
from datetime import datetime, timedelta
import json
import os
from time import sleep
from google.cloud import storage
from google.oauth2 import service_account
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import TimestampType
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=1),
    'retries': 1,
}
#configure kafka and gcs parameters
BOOTSTRAP_SERVERS = 'broker:29092'
TOPIC_NAME = 'api-data-topic'
COMSUMER_GROUP = 'datagov-consumer-group'
GCS_PROJECT_ID = 'datath-th-kafka'
BUCKET_NAME = "kafka_syce_gcp"
BUSINESS_DOMAIN = "datagov"
DESTINATION_FOLDER = f"{BUSINESS_DOMAIN}/raw"
PROCESSED_FOLDER = f"{BUSINESS_DOMAIN}/processed"
KEYFILE_PATH = '/opt/Key/datath-th-kafka-5dbcfce64148.json'


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
            with open(KEYFILE_PATH) as f:
                service_account_info = json.load(f)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            storage_client = storage.Client(credentials=credentials, project=GCS_PROJECT_ID)

            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)

            print(f"Uploaded {source_file_name} → gs://{bucket_name}/{destination_blob_name}")


@dag(default_args=default_args, schedule='@daily', start_date=datetime(2023, 8, 27), catchup=False)
def work_flow():
    @task()
    def consume_and_upload():
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id=COMSUMER_GROUP,
        )
            try:
                count = 0
                for message in consumer:
                    try:
                        data = json.loads(message.value.decode("utf-8"))
                        print(f"Received message: {data}")

                        # ตั้งชื่อไฟล์ตาม train_id + timestamp
                        train_id = data.get("id", "unknown")
                        now = int(datetime.now().timestamp())
                        file_name = f"{train_id}-{now}.json"

                        os.makedirs("/opt/airflow/data", exist_ok=True)
                        source_file_name = f"/opt/airflow/data/{file_name}"
                        
                        with open(source_file_name, "w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False)


                        upload_to_gcs(
                            bucket_name=BUCKET_NAME,
                            source_file_name=source_file_name,
                            destination_blob_name=f"{DESTINATION_FOLDER}/{file_name}",
                        )

                        sleep(3)
                        count += 1
                        if count >= 5:
                            break

                    except json.decoder.JSONDecodeError as e:
                        print(f"JSON decode error: {e}")
                        continue
            except KeyboardInterrupt:
                print("Stopping consumer...")
                consumer.close()
    @task()
    def transform_data():
        spark = SparkSession.builder.appName("datagov") \
            .master("local[*]") \
            .config("spark.jars", "/opt/airflow/docker/jars/gcs-connector-hadoop3-latest.jar") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "5g") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
            .getOrCreate()


        GSC_FILE_PATH = f"gs://{BUCKET_NAME}/{DESTINATION_FOLDER}/*.json"
        OUTPUT_PATH = f"gs://{BUCKET_NAME}/{PROCESSED_FOLDER }/"


        df_raw = spark.read.option("multiline", "true").json(GSC_FILE_PATH)
        df = df_raw.selectExpr("inline(result.records)")
        df = df.withColumn("วันที่", from_unixtime(col("วันที่")/1000).cast(TimestampType()))

        df.printSchema()
        df.show()

        df.createOrReplaceTempView("datagov")
        df = spark.sql("""
            select
                *

            from datagov
        """)
        df.write.mode("overwrite").parquet(OUTPUT_PATH)
        print(f"Data written successfully to {OUTPUT_PATH}")


    Bigquery = GCSToBigQueryOperator(
            task_id="upload_to_bigquery",
            bucket="kafka_syce_gcp",
            source_objects=["datagov/processed/*.parquet"],
            destination_project_dataset_table="datath-th-kafka.bigquery",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )

    consume_and_upload() >> transform_data() >> Bigquery
    
work_flow()