import os
import boto3
from dotenv import load_dotenv

load_dotenv()

sqs_client = boto3.client(
    'sqs',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)


def create_queue(queue_name):
    response = sqs_client.create_queue(QueueName=queue_name)
    print(response)
    return response['QueueUrl']


def send_message_to_queue(queue_url, message):
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=message
    )
    print(response)


def receive_message(queue_url):
    response = sqs_client.receive_message(
        QueueUrl=queue_url
    )
    print(response)


if __name__ == '__main__':
    q_url = create_queue("WeatherNewsQueue")
    send_message_to_queue(q_url, "Today we can expect rain at 4pm")
    receive_message(q_url)
