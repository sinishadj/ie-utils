import datetime
import inspect
import json
import logging
import os

import boto3
import sentry_sdk
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from sentry_sdk.utils import BadDsn

from ie_utils.constants import SENTRY_DSN_VAR_NAME, LOGGING_LEVEL_VAR_NAME, DYNAMO_DB_CONFIG_VAR_NAME


# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------                Logging utils                                  ---------------------------
# ---------------------------------------------------------------------------------------------------------------------

def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName(os.environ.get(LOGGING_LEVEL_VAR_NAME, 'INFO')))
    return logger


def init_sentry_sdk():
    try:
        dsn = os.environ.get(SENTRY_DSN_VAR_NAME)
        if dsn:
            sentry_sdk.init(
                dsn=dsn,
                integrations=[AwsLambdaIntegration()]
            )
    except BadDsn:
        get_logger().exception('Failed to log to sentry, bad dsn: {dsn}')


def capture_exception(exc):
    init_sentry_sdk()
    sentry_sdk.capture_exception(exc)


# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------                S3 utils                                       ---------------------------
# ---------------------------------------------------------------------------------------------------------------------

class S3Utils:
    """
    Util cass for aws s3 operations
    """
    S3_RESOURCE_NAME = 's3'

    @staticmethod
    def get_object(bucket_name, file_key):
        """
        Get s3 object by file key and bucket name

        :param bucket_name:
        :param file_key:
        :return:
        """
        s3_resource = boto3.resource(S3Utils.S3_RESOURCE_NAME)
        s3_object = s3_resource.Object(
            bucket_name,
            file_key
        )
        return s3_object

    @staticmethod
    def put_object(bucket_name, file_key, file_bytes):
        """
        Stores file to s3 bucket

        :param bucket_name:
        :param file_key:
        :param file_bytes:
        :return:
        """
        s3_client = boto3.client(S3Utils.S3_RESOURCE_NAME)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=file_bytes
        )


# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------                Dynamo DB utils                                ---------------------------
# ---------------------------------------------------------------------------------------------------------------------

class DynamoDBUtils:
    """
    Util cass for aws dynamo db operations
    """
    DYNAMO_DB_RESOURCE_NAME = 'dynamodb'

    @staticmethod
    def log_wrapper(class_instance, fn_name, table_name, table_key):
        func = getattr(class_instance, fn_name)

        def wrapper(*args, **kwargs):
            DynamoDBUtils.log(table_name, table_key, f'function {func.__name__} request',
                              {'signature': str(inspect.signature(func)), 'args': list(args), 'kwargs': kwargs})
            results = func(*args, **kwargs)
            DynamoDBUtils.log(table_name, table_key, f'function {func.__name__} response',
                              {'args': list(args), 'result': json.dumps(results)})
            return results

        setattr(class_instance, fn_name, wrapper)

    @staticmethod
    def log(table_name, table_key, description, log_object):
        if not (table_name and table_key and log_object):
            get_logger().error(
                f'Database logging impossible due to None value, '
                f'table name: {table_name}, table key: {table_key}, log_object: {log_object}'
            )
            return

        try:
            DynamoDBUtils.update_item(
                table_name,
                **{
                    'Key': {'identifier': table_key},
                    'UpdateExpression':
                        "SET log_messages = list_append(if_not_exists(log_messages, :empty_list), :add_value)",
                    'ExpressionAttributeValues': {
                        ':empty_list': [],
                        ':add_value': [{
                            'datetime': str(datetime.datetime.now()),
                            'description': description,
                            'log_object': str(log_object)
                        }]
                    }
                }
            )
        except Exception as e:
            get_logger().exception(f'Error logging event {description} to db, log object: {log_object}')
            capture_exception(e)

    @staticmethod
    def update_event(**kwargs):
        DynamoDBUtils.update_item(
            kwargs.get('table_name'),
            **{
                'Key': {'identifier': kwargs.get('table_key')},
                'UpdateExpression':
                    "SET message = :errorMessage, #st = :eventStatus, processed_at = :ts",
                'ExpressionAttributeValues': {
                    ':errorMessage': kwargs.get('errorMessage') if 'errorMessage' in kwargs else None,
                    ':eventStatus': kwargs.get('status') if 'status' in kwargs else None,
                    ':ts': str(datetime.datetime.now())
                },
                'ExpressionAttributeNames': {"#st": "status"}
            }
        )

    @staticmethod
    def update_item(table_name, **kwargs):
        table = DynamoDBUtils.get_table(table_name)
        table.update_item(**kwargs)

    @staticmethod
    def get_table(table_name):
        """
        Fetch dynamo db table based on table name

        :param table_name:
        :return:
        """
        dynamo_db = boto3.resource(DynamoDBUtils.DYNAMO_DB_RESOURCE_NAME,
                                   **json.loads(os.getenv(DYNAMO_DB_CONFIG_VAR_NAME, '{}')))
        return dynamo_db.Table(table_name)

    @staticmethod
    def deserialize_to_python_data(dynamo_db_dict: dict) -> dict:
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in dynamo_db_dict.items()}

    @staticmethod
    def serialize_python_data(python_data: dict) -> dict:
        serializer = TypeSerializer()
        return {k: serializer.serialize(v) for k, v in python_data.items()}

    @staticmethod
    def record_exists(table_name, search_key) -> bool:
        """
        Checks is a record already exists in dynamo db

        :param table_name: dynamo db table name
        :param search_key: key to search by
        :return: True, if event is present in dynamo db, False otherwise
        """
        table = DynamoDBUtils.get_table(table_name)
        item = table.get_item(Key=search_key) if table else None
        return item is not None and 'Item' in item

    @staticmethod
    def put_item(table_name, entry_data):
        """
        Put entry_data into dynamo db table with a given name

        :param table_name:
        :param entry_data:
        :return:
        """
        table = DynamoDBUtils.get_table(table_name)
        table and table.put_item(Item=entry_data)

    @staticmethod
    def get_items_by_search_attr(table_name, search_attr):
        table = DynamoDBUtils.get_table(table_name)
        item = table.scan(search_attr) if table else None
        return item['Items'] if 'Items' in item else None

    @staticmethod
    def get_item_by_search_key(table_name, search_key) -> dict:
        """
        Get item from dynamo db by search_key

        :param table_name: dynamo db table name
        :param search_key: key to search by
        :return: Item, if event is present in dynamo db, None otherwise
        """
        table = DynamoDBUtils.get_table(table_name)
        item = table.get_item(Key=search_key) if table else None
        return item['Item'] if 'Item' in item else None
