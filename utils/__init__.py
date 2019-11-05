import datetime
import gettext
import inspect
import json
import logging
import os
import re
from decimal import Decimal

import boto3
import pycountry
import sentry_sdk
from boto3.dynamodb.types import TypeDeserializer
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from sentry_sdk.utils import BadDsn

# TODO: replace the import of variables in each inclusion
from utils.constants import SENTRY_DSN_VAR_NAME, LOGGING_LEVEL_VAR_NAME, DYNAMO_DB_CONFIG_VAR_NAME


class InvoiceType:
    SALE = 'venta'
    PURCHASE = 'compra'
    SALE_RECTIFYING = 'rectifica_venta'
    PURCHASE_RECTIFYING = 'rectifica_compra'

    TYPES = (
        SALE,
        PURCHASE,
        SALE_RECTIFYING,
        PURCHASE_RECTIFYING
    )

    SALES = [SALE, SALE_RECTIFYING]
    PURCHASES = [PURCHASE, PURCHASE_RECTIFYING]
    RECTIFIES = [SALE_RECTIFYING, PURCHASE_RECTIFYING]


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
    def deserialize_python_data(record) -> dict:
        deserializer = TypeDeserializer()
        return {
            k: deserializer.deserialize(v) for k, v in
            record.get(DynamoDBUtils.DYNAMO_DB_RESOURCE_NAME).get('NewImage').items()
        }

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


class TxerpadParseUtils:
    @staticmethod
    # TODO: remove this from lambdas that don't need it ~10MB is the size of pycountry library
    def parse_country(country_text: str, country_translation_languages=()) -> str:
        """
        Converts alpha_3 code or full country name to alpha_2 code

        Parameters
        ----------
        country_text    a country text in alpha_3 format (https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) or name of
                        the country

        Returns         alpha_2 country code(https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)
        -------

        """

        def translate(en_country_text, transl_country_text):
            # try in english
            if en_country_text.lower() == transl_country_text.lower():
                return True
            # try in other predefined languages
            else:
                for language_code in country_translation_languages:
                    language_translator = gettext.translation('iso3166', pycountry.LOCALES_DIR,
                                                              languages=[language_code])
                    if language_translator.gettext(en_country_text).lower() == transl_country_text.lower():
                        return True
                return False

        alpha_2_countries = [country.alpha_2 for country in pycountry.countries if
                             country.alpha_3.lower() == country_text.lower() or translate(country.name, country_text)]
        return alpha_2_countries[0] if alpha_2_countries else None

    @staticmethod
    def parse_money(money):
        """
        Converts money in the text or number format into Decimal

        Parameters
        ----------
        money       an amount in the text or number format

        Returns     Decimal amount
        -------

        """
        if isinstance(money, (float, int)):
            return Decimal(money)
        elif isinstance(money, str):
            digits_text = money.replace(',', '.')
            digits_text = re.match(r'(\d|\.)+', digits_text).group()
            digit_arr = digits_text.split('.')
            return Decimal(f'{"".join(digit_arr[:-1])}.{digit_arr[-1]}') if len(digit_arr) > 1 else Decimal(digits_text)
        return None

    @staticmethod
    def parse_invoice_period(invoice_period, invoice_type, issue_date, date_format):
        """
        Converts invoice period to txerpad compatible invoice period
        Parameters
        ----------
        invoice_period      a trimester period, one of the following 1T, 2T, 3T, 4T
        invoice_type        type of the invoice
        issue_date          date of invoice issue

        Returns             txerpad compatible invoice period
        -------

        """
        if invoice_type in InvoiceType.SALES:
            return '%s%s' % (invoice_period, datetime.datetime.strptime(issue_date, date_format).year)
        elif invoice_type in InvoiceType.PURCHASES:
            curr_month = datetime.datetime.today().month
            curr_year = datetime.datetime.today().year
            curr_trimester = (curr_month - 1) // 3

            year = curr_year if int(invoice_period.replace('T', '')) <= curr_trimester else curr_year - 1
            return '%s%s' % (invoice_period, year)
        return None

    @staticmethod
    def parse_tax(invoice_type, tax_type, tax_rate):
        """
        Gets corresponding txerpad tax_code depending on the input parameters
        Parameters
        ----------
        invoice_type
        tax_type
        tax_rate

        Returns
        -------

        """
        if tax_type == 'IVA':
            if invoice_type in InvoiceType.SALES:
                # TODO: additional parameters for 0
                return {
                    0: 'IVANOSUJETO',
                    4: 'IVAVENTASE4',
                    10: 'IVAVENTASE10',
                    21: 'IVAVENTASE21'
                }.get(tax_rate, None)
            elif invoice_type in InvoiceType.PURCHASES:
                # TODO: additional parameters for 0
                return {
                    0: 'IVACOMPRASNOSUJETO',
                    0.5: 'IVACOMPRASRE05',
                    1.4: 'IVACOMPRASRE14',
                    5.2: 'IVACOMPRASRE52',
                    4: 'IVACOMPRASE4',
                    10: 'IVACOMPRASE10',
                    21: 'IVACOMPRASE21'
                }.get(tax_rate, None)
        elif tax_type == 'IRPF':
            if invoice_type in InvoiceType.SALES:
                return {
                    1: 'IRPFCUENTA1',
                    2: 'IRPFCUENTA2',
                    7: 'IRPFCUENTA7',
                    15: 'IRPFCUENTA15',
                    19: 'IRPFCUENTA19A',
                    19.5: 'IRPFCUENTA195A',
                    20: 'IRPFCUENTA20',
                    21: 'IRPFCUENTA21'
                }.get(tax_rate, None)
            elif invoice_type in InvoiceType.PURCHASES:
                return {
                    1: 'RETIRPF1',
                    2: 'RETIRPF2',
                    7: 'RETIRPF7',
                    15: 'RETIRPF15',
                    19: 'RETIRPF19A',
                    19.5: 'RETIRPF195A',
                    20: 'RETIRPF20',
                    21: 'RETIRPF21'
                }.get(tax_rate, None)
