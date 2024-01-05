'''
@ author: Amal Joseph Varghese
@ email: amaljova@gmail.com
@ github: https://github.com/amaljova
@ date: 29 June 2023
'''

import os
import sys
import boto3
import logging
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from pathlib import Path, PurePosixPath
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(module)-25s %(message)s',
    handlers=[
        # logging.FileHandler(),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("S3ClientLogger")


class Nutanix:
    def __init__(self):
        self.session = boto3.session.Session()

    def setClient(self, config):
        self.s3client = self.session.client(
            endpoint_url=config["ENDPOINT_URL"],
            aws_access_key_id=config["ACCESS_KEY"],
            aws_secret_access_key=config["SECRET_KEY"],
            service_name="s3",
            use_ssl=False,
        )

    def checkOrCreateBucket(self, bucket_name):
        self.bucket = bucket_name
        # check if bucket exists
        try:
            self.s3client.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket exists : {self.bucket}")
        except ClientError:
            logger.info(f"Bucket {self.bucket} does not exist.  " +
                  "Attempting to create bucket ...")
            try:
                self.s3client.create_bucket(Bucket=self.bucket)
                logger.info(f"Bucket {self.bucket} Created. ")

            except Exception as err:
                logger.info("An exception occurred while creating the " +
                      f"{self.bucket} bucket.  " + f"Details: {err}")
                sys.exit()

    # The upload_file method accepts a file name, a bucket name,
    # and an object name. The method handles large files by
    # splitting them into smaller chunks and uploading each chunk in parallel.

    def upload_file(self, file_name, object_name=None):
        """Upload a single file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        config = TransferConfig(
            multipart_threshold=5*(1024 ** 3),
            max_concurrency=10,
            use_threads=True,
        )
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        try:
            response = self.s3client.upload_file(file_name,self.bucket, object_name,
                                                 Config=config,
                                                 #    Callback=ProgressPercentage(file_name),
                                                 )
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def uploadRealtiveDirectory(self, source_path):
        '''
        This uploads an entire directory to an S3 bucket.
        Expecting relative path.
        '''   
        for file in tqdm(Path(source_path).rglob("*"), desc ="Uploading Files"):
            if file.is_file():
                destination = str(PurePosixPath(file))
                file = str(file)
                self.upload_file(file, destination)
        logger.info(f"Uploaded: {source_path}")

    def uploadDirectory(self, source_path):
        '''
        This uploads an entire directory tree to an S3 bucket.
        Expecting absolute/relative path.
        '''
        for my_path, dirs, files in tqdm(os.walk(source_path), desc ="Uploading Files"):
            dest_path = self.getDestPath(my_path, source_path)
            for file_name in files:
                source_file_path = os.path.join(my_path,file_name)
                dest_file_path = f"{dest_path}/{file_name}"
                self.upload_file(source_file_path, object_name = dest_file_path)
        logger.info(f"Uploaded: {source_path}")


    def getDestPath(self, root_from_walk_dir, source_path):
        "Get only the target folder name from absolute/relative path"
        source_path_destructured = str(PurePosixPath(Path(source_path))).split("/")
        root_from_walk_dir_destructured = str(PurePosixPath(Path(root_from_walk_dir))).split("/")
        dest_path = "/".join(root_from_walk_dir_destructured[len(source_path_destructured)-1:])
        return dest_path
