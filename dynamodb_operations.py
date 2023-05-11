import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from dotenv import load_dotenv

load_dotenv()

dynamodb_resource = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)


# create table
def create_table(table_name):
    table = dynamodb_resource.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'reg_no', 'KeyType': 'HASH'},   # partition key
            {'AttributeName': 'name', 'KeyType': 'RANGE'}    # sort key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'reg_no', 'AttributeType': 'N'},
            {'AttributeName': 'name', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    print(f"Successfully created table {table}")
    return table


# insert item
def insert_item(table, data_to_insert):
    response = table.put_item(
        Item=data_to_insert
    )
    print(f"Inserted item {response}")


# get existing table
def get_table(table_name):
    table = dynamodb_resource.Table(table_name)
    return table


# update item
def update_item(table):
    response = table.update_item(
        Key={
            'reg_no': 444,
            'name': 'Paran'
        },
        UpdateExpression='SET #dep=:dep',
        ExpressionAttributeValues={
            ':dep': '2023-09-09'
        },
        ExpressionAttributeNames={
            '#dep': 'dob'
        }
    )
    print(f"updated item {response}")
    return response


# query items
def query_table(table, table_name):
    response = table.query(
        TableName=table_name,
        KeyConditionExpression=Key('reg_no').eq(777)
    )
    print(f"Queried item {response}")
# scan items


def scan_table(table, table_name):
    response = table.scan(
        TableName=table_name,
        FilterExpression=Attr('department').eq('IT')
    )
    print(f"Scanned response {response}")


# delete item
def delete_item(table, hash_key, sort_key):
    table.delete_item(
        Key={
            'reg_no': hash_key,
            'name': sort_key
        }
    )
    print("Deleted item")
# delete table


def delete_table(table):
    table.delete()
    print("table deleted")



if __name__ == '__main__':
    table_name = 'first_year_students'
    # create_table('first_year_students')
    table = get_table(table_name)
    # data_to_insert = {
    #         'reg_no': 444,
    #         'name': 'Paran',
    #         'email': 'paran@gmail.com',
    #         'dob': '2023-06-06',
    #         'department': 'EEE'
    #     }
    # insert_item(table, data_to_insert)
    # update_item(table)
    # query_table(table, table_name)

    # scan_table(table, table_name)
    # delete_item(table, 444, 'Paran')
    delete_table(table)