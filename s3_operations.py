import json
import os
import boto3
from dotenv import load_dotenv

load_dotenv()


s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)

s3_resource = boto3.resource(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)


def create_bucket(bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Created bucket {bucket_name}")


def upload_file(local_filename, bucket, bucket_file_name):
    s3_client.upload_file(local_filename, bucket, bucket_file_name)
    print("Uploaded file successfully")


def download_file(bucket, bucket_file_name, local_filepath):
    response = s3_client.download_file(bucket, bucket_file_name, local_filepath)
    print(f"downloaded file successfully {response}")


def download_object(bucket_name, file_name):
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_name
    )
    data = response.get('Body').read().decode('utf-8')
    print(data)


def empty_bucket(bucket_name):
    bucket = s3_resource.Bucket(bucket_name)
    bucket.objects.all().delete()
    print(f"Emptied objects in bucket {bucket_name}")


def delete_bucket(bucket_name):
    s3_client.delete_bucket(Bucket=bucket_name)
    print("Bucket deleted successfully")


if __name__=='__main__':
    # create_bucket('python-training-s3-bucket-hlv')
    # upload_file('/home/paranthaman/Downloads/cereal.csv', 'python-training-s3-bucket-hlv', '/backup/cereal.csv')
    # download_file('python-training-s3-bucket-hlv', 'cereal.csv', '/home/paranthaman/Videos/s3files/cerealnew.csv')
    # download_object('python-training-s3-bucket-hlv', 'cereal.csv')
    empty_bucket('python-training-s3-bucket-hlv')
    delete_bucket('python-training-s3-bucket-hlv')


