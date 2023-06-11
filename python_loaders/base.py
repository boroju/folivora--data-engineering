from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Union
import json
import os.path as op
import yaml
import boto3
from botocore.exceptions import ClientError
from python_loaders.utils import logging


class BasePythonLoader(ABC):

    # The name is used by the CLI to be mapped with the class object
    name = None

    # This helps to abstract the config extraction for every child class
    # Assumes "config.yaml" in same folder of derived class. Can be customised.
    config_path = "config.yaml"

    def __init__(
            self,
            start_date: Union[str, int] = None,
            end_date: Union[str, int] = None,
            tenant: str = None
    ):
        self.start_date = self.parse_date(start_date)
        self.end_date = self.parse_date(end_date)
        self.tenant = tenant
        self.config = self.get_configuration()

        self.spark_session = None
        self.secret_manager = None
        self.s3 = None

    @abstractmethod
    def load(self):
        # The load function is an abstract method and it must be added to each
        # child class, and customized based on targeted API. This help keep
        # things unified for the CLI.
        ...

    def get_configuration(self, site='') -> Dict:
        import inspect
        configuration_path = op.join(
            op.dirname(inspect.getfile(self.__class__)),
            site,
            self.config_path
        )
        try:
            with open(configuration_path) as conf:
                return yaml.load(conf, Loader=yaml.FullLoader) or {}
        except FileNotFoundError:
            # no point in creating a yaml config if there is no need for one.
            # we accept the scenario and return empty dict instead
            logging.info(
                f"Config file '{self.config_path}' not found")
            return {}

    def get_secret(self, secret_name: str) -> Dict:
        # we create the secret manager if it was not created before
        if self.secret_manager is None:
            session = boto3.session.Session()
            self.secret_manager = session.client(
                service_name='secretsmanager',
                region_name="eu-west-1",  # todo: as env var?
            )
        try:
            secret_response = self.secret_manager.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            raise e

        # Only returning SecretString. todo: See if SecretBinary makes sense
        try:
            return json.loads(secret_response['SecretString'])
        # Allow getting string secrets
        except ValueError:
            return secret_response['SecretString']

    @staticmethod
    def parse_date(date_string: Union[str, None]) -> Union[datetime, None]:
        if date_string is None:
            return None
        formatting_error = (
            f"Format for {date_string} not understood. "
            f"Accepted format are '%Y%m%d' or '%Y%m%d%H' "
            f"(e.g. 20210328 or 2021032815)."
        )
        try:
            int(date_string)
        except ValueError:
            raise DateFormatException(formatting_error)

        date_string = str(date_string)
        if len(date_string) == 8:
            date_format = "%Y%m%d"
        elif len(date_string) == 10:
            date_format = "%Y%m%d%H"
        else:
            raise DateFormatException(formatting_error)

        return datetime.strptime(date_string, date_format)

    def upload_file_to_s3(
            self,
            file_name: str,
            bucket: str,
            object_name: str = None,
            s3_client=None
    ):
        if s3_client is not None:
            s3 = s3_client
        else:
            if self.s3 is None:
                self.s3 = boto3.client('s3')
            s3 = self.s3

        if object_name is None:
            object_name = file_name

        try:
            s3.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file_from_s3(
            self,
            bucket_name: str,
            object_name: str,
            filename: str,
            s3_client=None
    ):
        if s3_client is not None:
            s3 = s3_client
        else:
            if self.s3 is None:
                self.s3 = boto3.client('s3')
            s3 = self.s3
        s3.download_file(bucket_name, object_name, filename)
        return filename

    def list_s3_objects(self, bucket, prefix="", s3_client=None):
        # logic below is odd. may have to change at some point
        # basically want to enforce self.s3 to be s3 clients with permissions
        # in swamp apiloaders path, but sometimes, we may need to use an S3
        # client that is external (using aws keys) and still benefit from those
        # awesome methods.
        if s3_client is not None:
            s3 = s3_client
        else:
            if self.s3 is None:
                self.s3 = boto3.client('s3')
            s3 = self.s3

        s3_objects = []
        params = dict(Bucket=bucket, Prefix=prefix)
        while True:
            response = s3.list_objects_v2(**params)
            for s3_key in response["Contents"]:
                s3_objects.append(s3_key["Key"])

            if not response.get("NextContinuationToken"):
                break

            params["ContinuationToken"] = response["NextContinuationToken"]
        return s3_objects


class DateFormatException(Exception):
    """
    Raised when date format is not understood
    """


class TenantDoesNotExist(Exception):
    """
    Raised when tenant string given does not match one that was expected.
    """
