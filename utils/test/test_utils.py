import logging
import unittest

import mock

from utils import get_logger, init_sentry_sdk, S3Utils, DynamoDBUtils, \
    capture_exception
from utils.constants import SENTRY_DSN_VAR_NAME


class TestLogUtils(unittest.TestCase):
    @mock.patch('utils.os')
    def test_get_logger(self, os_mock):
        os_mock.environ.get.return_value = 'INFO'
        logger = get_logger()
        self.assertEqual('INFO', logging.getLevelName(logger.level))

    @mock.patch('utils.os')
    @mock.patch('utils.sentry_sdk')
    def test_init_sentry_sdk(self, sentry_sdk_mock, os_mock):
        os_mock.environ = {SENTRY_DSN_VAR_NAME: 'sentry_dsn'}
        init_sentry_sdk()
        self.assertEqual('sentry_dsn', sentry_sdk_mock.init.call_args[1].get('dsn'))

    @mock.patch('utils.init_sentry_sdk')
    @mock.patch('utils.sentry_sdk.capture_exception')
    def test_capture_exception(self, capture_exception_mock, init_sentry_sdk_mock):
        exc = Exception('error')

        capture_exception(exc)

        init_sentry_sdk_mock.assert_called()
        capture_exception_mock.assert_called()
        self.assertEqual(exc, capture_exception_mock.call_args[0][0])


class TestS3Utils(unittest.TestCase):
    @mock.patch('utils.boto3')
    def test_get_object(self, boto3_mock):
        s3_object_mock = mock.Mock()
        boto3_mock.resource.return_value = s3_object_mock

        S3Utils.get_object('bucket_name', 'file_key')

        s3_object_mock.Object.assert_called()
        self.assertEqual('bucket_name', s3_object_mock.Object.call_args[0][0])
        self.assertEqual('file_key', s3_object_mock.Object.call_args[0][1])

    @mock.patch('utils.boto3')
    def test_put_object(self, boto3_mock):
        s3_client_mock = mock.Mock()
        boto3_mock.client.return_value = s3_client_mock

        S3Utils.put_object('bucket_name', 'file_key', 'bytes'.encode('utf-8'))

        s3_client_mock.put_object.assert_called()
        self.assertEqual('bucket_name', s3_client_mock.put_object.call_args[1]['Bucket'])
        self.assertEqual('file_key', s3_client_mock.put_object.call_args[1]['Key'])
        self.assertEqual('bytes'.encode('utf-8'), s3_client_mock.put_object.call_args[1]['Body'])


class TestDynamoDBUtils(unittest.TestCase):

    @mock.patch('utils.DynamoDBUtils.log')
    def test_log_wrapper(self, log_mock):
        class Test:
            def test(self):
                return 'test'

        test_class_instance = Test()

        DynamoDBUtils.log_wrapper(test_class_instance, 'test', 'table_name', 'table_key')

        self.assertEqual('test', test_class_instance.test())
        self.assertEqual(2, log_mock.call_count)

    # ------------------------------------------------------------------------------------------------
    #   log
    # ------------------------------------------------------------------------------------------------

    @mock.patch('utils.DynamoDBUtils.update_item')
    @mock.patch('utils.datetime.datetime')
    def test_log(self, datetime_mock, update_item_mock):
        datetime_mock.now.return_value = 'now'

        DynamoDBUtils.log('table_name', 'table_key', 'log description', 'log object')

        update_item_mock.assert_called()
        self.assertEqual('table_name', update_item_mock.call_args[0][0])
        self.assertEqual({'identifier': 'table_key'}, update_item_mock.call_args[1]['Key'])
        self.assertEqual("SET log_messages = list_append(if_not_exists(log_messages, :empty_list), :add_value)",
                         update_item_mock.call_args[1]['UpdateExpression'])
        self.assertEqual(
            {
                ':empty_list': [],
                ':add_value': [{
                    'datetime': 'now',
                    'description': 'log description',
                    'log_object': 'log object'
                }]
            },
            update_item_mock.call_args[1]['ExpressionAttributeValues']
        )

    @mock.patch('utils.get_logger')
    def test_log_table_None(self, get_logger_mock):
        DynamoDBUtils.log(None, 'table_key', 'log description', 'log object')

        self.assertEqual(
            'Database logging impossible due to None value, '
            'table name: None, table key: table_key, log_object: log object',
            get_logger_mock.return_value.error.call_args[0][0])

    @mock.patch('utils.datetime.datetime')
    @mock.patch('utils.DynamoDBUtils.update_item')
    def test_update_event(self, update_item_mock, datetime_mock):
        datetime_mock.now.return_value = 'now'

        DynamoDBUtils.update_event(
            table_name='table_name',
            table_key='table_key',
            errorMessage='error message',
            status='status'
        )

        update_item_mock.assert_called()
        self.assertEqual('table_name', update_item_mock.call_args[0][0])
        self.assertEqual(
            {'ExpressionAttributeNames': {'#st': 'status'},
             'ExpressionAttributeValues': {':errorMessage': 'error message',
                                           ':eventStatus': 'status',
                                           ':ts': 'now'},
             'Key': {'identifier': 'table_key'},
             'UpdateExpression': 'SET message = :errorMessage, #st = :eventStatus, '
                                 'processed_at = :ts'},
            update_item_mock.call_args[1]
        )

    @mock.patch('utils.DynamoDBUtils')
    def test_update_item(self, dynamo_db_utils_mock):
        table_mock = mock.Mock()
        dynamo_db_utils_mock.get_table.return_value = table_mock

        DynamoDBUtils.update_item('table name', arg='arg')

        self.assertEqual('table name', dynamo_db_utils_mock.get_table.call_args[0][0])
        self.assertEqual('arg', table_mock.update_item.call_args[1]['arg'])

    @mock.patch('utils.boto3')
    def test_get_table(self, boto3_mock):
        DynamoDBUtils.get_table('table name')

        boto3_mock.resource.assert_called()
        self.assertEqual('table name', boto3_mock.resource.return_value.Table.call_args[0][0])

    def test_deserialize_python_data(self):
        records = {
            'eventName': 'INSERT',
            'dynamodb': {
                'NewImage': {
                    'identifier': {'S': 'id'},
                    "source": {'S': "stripe"},
                    'date_time': {'S': '2019-05-31'},
                    'status': {'S': 'processed'},
                    'body': {'S': '{"test":"test"}'},
                    'external_id': {'S': 'external_id'},
                    'message': {'S': 'no error message'},
                    'processed_at': {'S': '2019-05-31'}
                }
            }
        }

        python_data = DynamoDBUtils.deserialize_python_data(records)

        self.assertEqual({'body': '{"test":"test"}',
                          'date_time': '2019-05-31',
                          'external_id': 'external_id',
                          'identifier': 'id',
                          'message': 'no error message',
                          'processed_at': '2019-05-31',
                          'source': 'stripe',
                          'status': 'processed'},
                         python_data)

    @mock.patch('utils.DynamoDBUtils.get_table')
    def test_record_exists(self, get_table_mock):
        get_table_mock.return_value.get_item.return_value = ['Item']
        self.assertTrue(DynamoDBUtils.record_exists('table name', 'search key'))
