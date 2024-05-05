# this logging object can be imported directly
import backoff as backoff
import logging
import requests
from typing import Dict, List
from datetime import datetime
from requests.exceptions import RequestException
import os

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


# Define a function to find the project root directory
def find_project_root_path():
    current_dir = os.path.abspath(os.path.dirname(__file__))

    # Define the marker file or directory
    marker_file = '.gitignore'

    # Navigate up the directory tree until the marker file is found
    while current_dir != '/':
        if marker_file in os.listdir(current_dir):
            return current_dir
        current_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

    # If marker file is not found, return None or raise an exception
    return None


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
