import os
import boto3
from dotenv import load_dotenv

load_dotenv()

sns_client = boto3.client(
    'sns',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)


def create_topic(topic_name):
    topic = sns_client.create_topic(Name=topic_name)
    print(f"Created topic {topic}")
    return topic['TopicArn']


def subscribe_to_topic(topic_name, protocol, endpoint):
    sns_client.subscribe(
        TopicArn=topic_name,
        Protocol=protocol,
        Endpoint=endpoint
    )
    print(f"Subscribed successfully to the topic {topic_name}")


def publish_message_to_topic(topic, message, subject):
    response = sns_client.publish(
        TopicArn=topic,
        Message=message,
        Subject=subject
    )
    print("Published message", response)


if __name__ == '__main__':
    topic = create_topic('CinemaNews')
    # subscribe_to_topic(topic, 'sqs', 'arn:aws:sqs:us-east-1:610967138959:FlashMessageQueue')
    subscribe_to_topic(topic, 'email', 'venkat@gmail.com')

    publish_message_to_topic(topic, "Avatar 3 is releasing tomorrow", "Avatar3")