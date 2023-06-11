import logging
import backoff
import requests
from requests.exceptions import RequestException
import os
from datetime import datetime
from typing import Dict


# this logging object can be imported directly
logging.basicConfig(level=logging.INFO)


class TooManyRequestsException(Exception):
    """
    Raise when too many request sent to API
    """


class BearerAuthApi:

    def __init__(self, api_key, host):
        self.api_key = api_key
        self.host = host

    def create_header(self, extra_headers: Dict = None):
        header = {
            "Authentication": f"bearer {self.api_key}"
        }
        if extra_headers:
            header.update(extra_headers)
        return header

    @backoff.on_exception(backoff.expo, (RequestException,
                                         TooManyRequestsException))
    def get(self, endpoint, params=None, extra_headers: Dict = None, **kwargs):
        url = self.host + endpoint
        print(url)
        r = requests.get(
            url=url,
            params=params,
            headers=self.create_header(extra_headers),
            **kwargs
        )
        if r.status_code == 429:
            raise TooManyRequestsException
        return r

    @backoff.on_exception(backoff.expo, (RequestException,
                                         TooManyRequestsException))
    def post(self, endpoint, data=None, extra_headers: Dict = None, **kwargs):
        url = self.host + endpoint
        r = requests.post(
            url=url,
            data=data,
            headers=self.create_header(extra_headers),
            **kwargs
        )
        if r.status_code == 429:
            raise TooManyRequestsException
        return r


class BasicAPIKeyAuth:

    def __init__(self, api_key, host):
        self.api_key = api_key
        self.host = host

    def create_header(self, extra_headers: Dict = None):
        header = {
            "Authorization": f"Basic {self.api_key}"
        }
        if extra_headers:
            header.update(extra_headers)
        return header

    @backoff.on_exception(backoff.expo, (RequestException,
                                         TooManyRequestsException))
    def get(self, endpoint, params=None, extra_headers: Dict = None, **kwargs):
        url = self.host + endpoint
        r = requests.get(
            url=url,
            params=params,
            headers=self.create_header(extra_headers),
            **kwargs
        )
        if r.status_code == 429:
            raise TooManyRequestsException
        return r

    @backoff.on_exception(backoff.expo, (RequestException,
                                         TooManyRequestsException))
    def post(self, endpoint, data=None, extra_headers: Dict = None, **kwargs):
        url = self.host + endpoint
        r = requests.post(
            url=url,
            data=data,
            headers=self.create_header(extra_headers),
            **kwargs
        )
        if r.status_code == 429:
            raise TooManyRequestsException
        return r


class ConfigPath:

    environment = os.environ.get("ENVIRONMENT")
    products = f"s3a://data-sch-products-{environment}"
    landing = f"s3a://data-lake-landing-{environment}"
    lake = f"s3a://data-sch-lake-{environment}"
    apiloaders_path = "dwh/apiloaders"

    def create_path(self, bucket: str):
        bucket_type = getattr(ConfigPath, bucket)
        return f"{bucket_type}/{self.apiloaders_path}"

    @staticmethod
    def get_bucket(bucket: str):
        bucket_type = getattr(ConfigPath, bucket)
        return bucket_type.replace("s3a://", "")


def convert_to_datetime(timestamp_string):
    """
    To convert string to datetime in the panda dataframe extracted.
    Sometimes the string contains the milliseconds, sometimes not. This is to
    ensure we are parsing the time even if it has multiple formats possible.
    :param timestamp_string: timestamp string
    :return: datetime
    """
    if not timestamp_string:
        return None
    timestamp_string = timestamp_string.upper()
    if timestamp_string[-1] != "Z":
        timestamp_string += "Z"

    potential_formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%MZ",
        "%Y-%m-%dT%HZ"
    ]
    for fmt in potential_formats:
        try:
            return datetime.strptime(timestamp_string, fmt)
        except ValueError:
            continue
    return None
