import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google_cloud/ServiceKey_GoogleCloud.json'

def upload_to_bucket(blob_name, data, bucket_name, content_type = 'text/csv'):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)

def download_from_bucket(blob_name, bucket_name, file_name):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(file_name)

