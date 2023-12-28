import os
import boto3
from . import utils
import pandas as pd
from io import StringIO
from typing import Dict, Any, List


class CloudStorage:
    def __init__(self):

        self.s3 = boto3.resource(
            's3',
            endpoint_url=os.environ.get("MINIO_ENDPOINT"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY")
        )
        self.logger = utils.setup_logger(__name__)

    def upload_to_bucket(self, bucket: str, file: Any, file_name: str, format: str) -> None:
        try:
            if format == "json":
                self._upload_json(bucket, file, file_name)
            elif format == "csv":
                self._upload_csv(bucket, file, file_name)
        except Exception as e:
            self.logger.exception(
                f"Sth Unexpected happened while trying to upload '{file_name}' to '{bucket}' due to: {str(e)}.")

    def _upload_json(self, bucket: str, file: Dict[str, Any], file_name: str) -> None:
        try:
            self.s3.meta.client.put_object(
                Bucket=bucket,
                Key=f'{file_name}',
                Body=file.encode('utf-8'),
                ContentType='application/json'
            )
        except Exception as e:
            self.logger.exception(
                f"Uploading '{file_name}' to '{bucket}' was unsuccessful due to: {str(e)}.")

    def _upload_csv(self, bucket: str, file: pd.DataFrame, file_name: str) -> None:
        try:
            csv_buffer = StringIO()
            file.to_csv(csv_buffer, encoding='utf-8', index=False)
            self.s3.Object(bucket, f'{file_name}').put(
                Body=csv_buffer.getvalue())
        except Exception as e:
            self.logger.exception(
                f"Uploading '{file_name}' to '{bucket}' was unsuccessful due to: {str(e)}.")

    def get_files(self, bucket: str) -> List[str]:
        try:
            bucket_s3 = self.s3.Bucket(bucket)
            files = [file.key for file in bucket_s3.objects.all()]
            return files
        except Exception as e:
            self.logger.exception(
                f"Sth Unexpected happened, while getting keys from '{bucket}' due to: {str(e)}.")

    def get_object(self, bucket: str, key: str) -> Dict[str, Any]:
        try:
            object = self.s3.meta.client.get_object(Bucket=bucket, Key=key)
            data = object['Body'].read().decode('utf-8')
            return data
        except Exception as e:
            self.logger.exception(
                f"Sth Unexpected happened, while getting files from '{bucket}' due to: {str(e)}.")

    def clean_bucket(self, bucket: str) -> None:
        try:
            bucket_s3 = self.s3.Bucket(bucket)
            bucket_s3.objects.all().delete()
        except Exception as e:
            self.logger.exception(
                f"Sth Unexpected happened, while cleaning '{bucket}' due to: {str(e)}.")

    def download_files(self, bucket: str, key: str) -> None:
        try:
            self.s3.meta.client.download_file(
                bucket, key, f'tmp/{key}.csv')
        except Exception as e:
            self.logger.exception(
                f"Sth Unexpected happened, while downloading files from '{bucket}' due to: {str(e)}.")
