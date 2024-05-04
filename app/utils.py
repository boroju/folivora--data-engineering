# this logging object can be imported directly
import backoff as backoff
import logging
import requests
from typing import Dict, List
from requests.exceptions import RequestException

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
