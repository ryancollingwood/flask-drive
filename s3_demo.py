import boto3
from pathlib import Path

def upload_file(file_name, bucket):
    """
    Function to upload a file to an S3 bucket
    """
    object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)

    return response


def download_file(file_name, bucket, target_path):
    """
    Function to download a given file from an S3 bucket
    """
    s3 = boto3.resource('s3')
    output = f"{target_path}/{file_name}"
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    s3.Bucket(bucket).download_file(file_name, output)

    return output


def list_files(bucket):
    """
    Function to list files in a given S3 bucket
    """
    s3 = boto3.client('s3')
    contents = []
    try:
        for item in s3.list_objects(Bucket=bucket)['Contents']:
            print(item)
            contents.append(item)
    except Exception as e:
        pass

    return contents

def list_files_with_prefix(prefix, bucket):
    """
    This is a potentially dangerous and/or flaky way to do this
    Given there could any number of items on a path, this method may:
        - Return a LOT of results
        - Crap itself due to the number of results
        - Crap itself because it looses it's place
    """
    s3 = boto3.client('s3')
    contents = []

    s3 = boto3.resource('s3')
    bucket_obj = s3.Bucket(name=bucket)
    for objects in bucket_obj.objects.filter(Prefix=prefix):
        print(objects.key)
        contents.append(objects.key)
    
    return contents