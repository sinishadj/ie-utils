# Integration Engine Utils

Python utility functions for Amazon DynamoDB, Amazon S3, and logging

* These function require aws credentials to be set on the local machine. How to do this is
described in: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html

* Furthermore, a set of os environment variables need to be set. Specifically, variable
names defined by constants defined in ```constants.py``` need to be set:
    * ```os.environ[LOGGING_LEVEL_VAR_NAME] =``` One of the logging levels, e.g., INFO, ERROR, DEBUG
    * ```os.environ[SENTRY_DSN_VAR_NAME] =```  Sentry Data Source Name, for logging exceptions (https://docs.sentry.io/error-reporting/quickstart/?platform=javascript#configure-the-sdk)
    * ```os.environ[DYNAMO_DB_CONFIG_VAR_NAME] =``` Dynamo Database configuration, example show below:
    ```
    {"region_name": "us-west-2", "endpoint_url": "http://localhost:8000", "aws_access_key_id": "local", "aws_secret_access_key": "local"}
    ```  