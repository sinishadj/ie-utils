import datetime
import logging
import unittest
from decimal import Decimal

import mock

from utils import get_logger, init_sentry_sdk, TxerpadParseUtils, InvoiceType, S3Utils, DynamoDBUtils, \
    capture_exception
from utils.constants import SENTRY_DSN_VAR_NAME


class TestUtils(unittest.TestCase):
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


class TestTxerpadParseUtils(unittest.TestCase):

    def setUp(self) -> None:
        self.SLASH_DATE_FORMAT = '%d/%m/%Y'
        self.COUNTRY_TRANSLATION_LANGUAGES = (
            'es',
            'de',
        )

    # ------------------------------------------------------------------------------------------------
    #   parse_country
    # ------------------------------------------------------------------------------------------------

    def test_parse_country_alpha_3(self):
        self.assertEqual('DE', TxerpadParseUtils.parse_country('DEU', self.COUNTRY_TRANSLATION_LANGUAGES))

    def test_parse_country_english(self):
        self.assertEqual('DE', TxerpadParseUtils.parse_country('Germany', self.COUNTRY_TRANSLATION_LANGUAGES))

    def test_parse_country_spanish(self):
        self.assertEqual('DE', TxerpadParseUtils.parse_country('Alemania', self.COUNTRY_TRANSLATION_LANGUAGES))

    def test_parse_country_german(self):
        self.assertEqual('DE', TxerpadParseUtils.parse_country('Deutschland', self.COUNTRY_TRANSLATION_LANGUAGES))

    # ------------------------------------------------------------------------------------------------
    #   parse_money
    # ------------------------------------------------------------------------------------------------

    def test_parse_money_int(self):
        self.assertEqual(Decimal(10), TxerpadParseUtils.parse_money(10))

    def test_parse_money_float(self):
        self.assertEqual(Decimal(10.58175), TxerpadParseUtils.parse_money(10.58175))

    def test_parse_money_text_curr(self):
        self.assertEqual(Decimal(10), TxerpadParseUtils.parse_money('10EUR'))

    def test_parse_money_text_symbol(self):
        self.assertEqual(Decimal(10), TxerpadParseUtils.parse_money('10€'))

    def test_parse_money_comma_removal(self):
        self.assertEqual(Decimal('158000.157'), TxerpadParseUtils.parse_money('158,000.157'))
        self.assertEqual(Decimal('158000.157'), TxerpadParseUtils.parse_money('158.000,157'))

    # ------------------------------------------------------------------------------------------------
    #   parse_invoice_period
    # ------------------------------------------------------------------------------------------------

    def test_parse_invoice_period_sale(self):
        expected, invoice_period, issue_date = ("1T2019", "1T", "25/01/2019")
        self.assertEqual(expected, TxerpadParseUtils.parse_invoice_period(invoice_period, InvoiceType.SALE, issue_date,
                                                                          self.SLASH_DATE_FORMAT))
        expected, invoice_period, issue_date = ("3T2015", "3T", "12/05/2015")
        self.assertEqual(expected, TxerpadParseUtils.parse_invoice_period(invoice_period, InvoiceType.SALE, issue_date,
                                                                          self.SLASH_DATE_FORMAT))
        expected, invoice_period, issue_date = ("2T2017", "2T", "10/10/2017")
        self.assertEqual(expected, TxerpadParseUtils.parse_invoice_period(invoice_period, InvoiceType.SALE, issue_date,
                                                                          self.SLASH_DATE_FORMAT))

    def test_parse_invoice_period_purchase_same_year(self):
        date_to_test_against = datetime.datetime.strptime('01/04/2019', '%d/%m/%Y')
        with mock.patch('utils.datetime.datetime') as datetime_mock:
            datetime_mock.today.return_value = date_to_test_against
            self.assertEqual('1T2019', TxerpadParseUtils.parse_invoice_period('1T', InvoiceType.PURCHASE, '01/01/2019',
                                                                              self.SLASH_DATE_FORMAT))

    def test_parse_invoice_period_purchase_prev_year(self):
        date_to_test_against = datetime.datetime.strptime('01/02/2019', '%d/%m/%Y')
        with mock.patch('utils.datetime.datetime') as datetime_mock:
            datetime_mock.today.return_value = date_to_test_against
            self.assertEqual('1T2018', TxerpadParseUtils.parse_invoice_period('1T', InvoiceType.PURCHASE, '01/01/2019',
                                                                              self.SLASH_DATE_FORMAT))

    def test_parse_invoice_period_None(self):
        self.assertIsNone(
            TxerpadParseUtils.parse_invoice_period('1T', 'NONEXISTENT_TYPE', '01/01/2019', self.SLASH_DATE_FORMAT))

    # ------------------------------------------------------------------------------------------------
    #   parse_tax
    # ------------------------------------------------------------------------------------------------

    def test_parse_tax_IVA_SALE(self):
        self.assertEqual('IVAVENTASE4', TxerpadParseUtils.parse_tax(InvoiceType.SALE, 'IVA', 4))

    def test_parse_tax_IVA_PURCHASE(self):
        self.assertEqual('IVACOMPRASE21', TxerpadParseUtils.parse_tax(InvoiceType.PURCHASE, 'IVA', 21))

    def test_parse_tax_IRPF_SALE(self):
        self.assertEqual('IRPFCUENTA195A', TxerpadParseUtils.parse_tax(InvoiceType.SALE, 'IRPF', 19.5))

    def test_parse_tax_IRPF_PURCHASE(self):
        self.assertEqual('RETIRPF15', TxerpadParseUtils.parse_tax(InvoiceType.PURCHASE, 'IRPF', 15))


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
