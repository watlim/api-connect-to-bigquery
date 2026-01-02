import json
import os
from datetime import datetime
from time import sleep

from google.cloud import storage
from google.oauth2 import service_account
from kafka import KafkaConsumer

# -----------------------------
# config สำหรับ kafka บน docker
# -----------------------------
BOOTSTRAP_SERVERS = "broker:29092"   # หรือ "kafka:9092" ถ้าอยู่ใน docker network เดียวกัน
TOPIC_NAME = "api-data-topic"
CONSUMER_GROUP = "datagov-consumer-group"

# -----------------------------
# config สำหรับ GCP
# -----------------------------
GCP_PROJECT_ID = "datath-th-kafka"
BUCKET_NAME = "kafka_syce_gcp"
BUSINESS_DOMAIN = "datagov"
DESTINATION_FOLDER = f"{BUSINESS_DOMAIN}/raw"
KEYFILE_PATH = "/opt/Key/datath-th-kafka-5dbcfce64148.json"

# -----------------------------
# kafka consumer (docker)
# -----------------------------
consumer = KafkaConsumer(
    TOPIC_NAME ="api-data-topic",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest"
)

# -----------------------------
# ฟังก์ชันอัพโหลดไฟล์ขึ้น GCS
# -----------------------------
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    with open(KEYFILE_PATH) as f:
        service_account_info = json.load(f)

    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    storage_client = storage.Client(project=GCP_PROJECT_ID, credentials=credentials)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(f"Uploaded {source_file_name} → gs://{bucket_name}/{destination_blob_name}")


# -----------------------------
# consume messages from kafka docker
# -----------------------------
try:
    for message in consumer:
        try:
            data = json.loads(message.value.decode("utf-8"))
            print(f"Received message: {data}")

            # ตั้งชื่อไฟล์ตาม train_id + timestamp
            train_id = data.get("train_id", "unknown")
            now = int(datetime.now().timestamp())
            file_name = f"{train_id}-{now}.json"

            os.makedirs("data", exist_ok=True)
            source_file_name = f"data/{file_name}"
            with open(source_file_name, "w") as f:
                json.dump(data, f)

            upload_to_gcs(
                bucket_name=BUCKET_NAME,
                source_file_name=source_file_name,
                destination_blob_name=f"{DESTINATION_FOLDER}/{file_name}",
            )

            sleep(3)

        except json.decoder.JSONDecodeError:
            print("Invalid JSON message, skipped")
            continue

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
