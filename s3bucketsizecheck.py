# Title: S3 Bucket Size Check
# Description: This is a simple tool to extract the size of each folder of a given S3 bucket.
#
# References:
#  - https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python
#  - https://stackoverflow.com/questions/35803027/retrieving-subfolders-names-in-s3-bucket-from-boto3
#  - https://stackoverflow.com/questions/32192391/how-do-i-find-the-total-size-of-my-aws-s3-storage-bucket-or-folder
#  - https://towardsdatascience.com/a-simple-guide-to-command-line-arguments-with-argparse-6824c30ab1c3

import boto3
import math
import argparse

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

def get_folder_size(bucket, prefix):
    total_size = 0
    for obj in boto3.resource('s3').Bucket(bucket).objects.filter(Prefix=prefix):
        total_size += obj.size
    return convert_size(total_size)

def get_bucket_root_size(bucket):
    paginator = boto3.client('s3').get_paginator('list_objects')
    iterator = paginator.paginate(Bucket=bucket, Prefix='', Delimiter='/', PaginationConfig={'PageSize': None})
    total_size = 0
    for response_data in iterator:
        contents = response_data.get('Contents', [])
        for content in contents:
            total_size += content['Size']
    return convert_size(total_size)

def list_folders_in_bucket(bucket):
    paginator = boto3.client('s3').get_paginator('list_objects')
    folders = []
    iterator = paginator.paginate(Bucket=bucket, Prefix='', Delimiter='/', PaginationConfig={'PageSize': None})
    for response_data in iterator:
        prefixes = response_data.get('CommonPrefixes', [])
        for prefix in prefixes:
            prefix_name = prefix['Prefix']
            if prefix_name.endswith('/'):
                folders.append(prefix_name.rstrip('/'))
    return folders

def list_folder_size(bucket):
    folders = list_folders_in_bucket(bucket)
    print('(Root): %s' % (get_bucket_root_size(bucket)))
    for folder in folders:
        print('%s: %s' % (folder, get_folder_size(bucket, folder)))

parser = argparse.ArgumentParser()
parser.add_argument('--bucket-name', type=str, required=True)
args = parser.parse_args()

list_folder_size(args.bucket_name)