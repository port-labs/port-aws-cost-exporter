import csv
import gzip
import logging
from datetime import datetime, timedelta, timezone
from io import StringIO

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self):
        self.s3 = boto3.resource("s3")

    def get_files(self, bucket_name, file_key_prefix, last_modified_in_days=1, key_suffix=".csv.gz"):
        for obj in self.s3.Bucket(bucket_name).objects.filter(Prefix=file_key_prefix):
            file_prefix_check = obj.key.endswith(key_suffix)
            last_modified_check = obj.last_modified > (datetime.now(timezone.utc) -
                                                       timedelta(days=last_modified_in_days))
            logger.info(f'Check S3 file: {bucket_name}/{obj.key}, last_modified: {obj.last_modified}; '
                        f'file_prefix_check: {file_prefix_check}, last_modified_check: {last_modified_check}')
            if all([file_prefix_check, last_modified_check]):
                yield obj

    @staticmethod
    def get_csv_gzipped_file_lines(obj):
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipped_csv_file:
            csv_reader = csv.reader(StringIO(gzipped_csv_file.read().decode()))
            for line in csv_reader:
                yield line
