'''
@ author: Amal Joseph Varghese
@ email: amaljova@gmail.com
@ github: https://github.com/amaljova
@ date: 30 June 2023
'''


from nutanix import Nutanix, logger
from dotenv import dotenv_values
import multiprocessing
from tqdm import tqdm
import os

configuration = dotenv_values(".env")

# Edit the script

def uploadDirectory(source_path):
    '''
    This uploads an entire directory to an S3 bucket recursively.
    '''

    nutanix_obj = Nutanix()
    nutanix_obj.setClient(configuration)
    nutanix_obj.checkOrCreateBucket("test-bucket")
    nutanix_obj.uploadDirectory(source_path)


if __name__ == "__main__":
    sources = [
        #"source_path",
        "./test/folder",
        ]
    # for cuncurrency
    with multiprocessing.Pool(processes=4) as p:
        p.map(uploadDirectory, sources)
    logger.info("All Done!")
