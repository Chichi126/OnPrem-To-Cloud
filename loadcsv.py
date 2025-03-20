from google.cloud import storage
import os

# Setting up the credential path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/amdari/apex-env/infra-cloud/config.json"

def upload_files(bucket_name, source_files, destination_name=None):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        for file in source_files:
            dest_name = destination_name if destination_name else os.path.basename(file)
            blob = bucket.blob(dest_name)
            blob.upload_from_filename(file)
            print(f"{file} uploaded to {bucket_name}/{dest_name}")

        return True

    except Exception as e:
        print(f"Error uploading files: {e}")
        return False

# Example usage
upload_files(
    "apexamdaribucket",
    ["/home/amdari/apex-env/data/products.csv", "/home/amdari/apex-env/data/regions.csv"]
)
