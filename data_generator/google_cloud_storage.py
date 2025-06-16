from google.cloud import storage


def upload_to_bucket(blob_name: str, data: str | bytes, bucket_name: str, content_type: str = "text/csv") -> None:
    """
    Upload data to a Google Cloud Storage bucket.

    Args:
        blob_name (str): Name of the blob/file to create or overwrite.
        data (str | bytes): Data to upload, either as a string or bytes.
        bucket_name (str): Name of the bucket to upload to.
        content_type (str, optional): MIME type of the data. Defaults to 'text/csv'.

    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)


def download_from_bucket(blob_name: str, bucket_name: str, file_name: str) -> None:
    """
    Download a blob/file from a Google Cloud Storage bucket to a local file.

    Args:
        blob_name (str): Name of the blob to download.
        bucket_name (str): Name of the bucket containing the blob.
        file_name (str): Local file path to save the downloaded blob.

    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(file_name)
