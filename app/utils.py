from minio import Minio
import os


def get_minio_client():
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )


def create_bucket_if_not_exists(bucket_name="data"):
    client = get_minio_client()

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created")
    else:
        print(f"Bucket '{bucket_name}' already exists")


def upload_raw_files(bucket_name="data"):
    client = get_minio_client()

    local_data_path = "/data"

    print("Listing files inside /data:", os.listdir(local_data_path))

    for file in os.listdir(local_data_path):
        if file.endswith(".csv"):

            object_name = f"raw/{file}"

            print("Uploading:", file)

            client.fput_object(
                bucket_name,
                object_name,
                os.path.join(local_data_path, file)
            )

            print(f"Uploaded {file} → {object_name}")
