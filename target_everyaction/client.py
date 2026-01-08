from hotglue_singer_sdk.target_sdk.client import HotglueSink
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
import requests
from hotglue_singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
import singer
from target_everyaction.auth import EveryActionAuth
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import backoff
import json

LOGGER = singer.get_logger()


class EveryActionSink(HotglueSink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.__auth = EveryActionAuth(self.config["app_name"], self.config["api_key"])

    @property
    def base_url(self):
        return "https://api.securevan.com/v4/"

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [409]:
            msg = response.reason
            raise FatalAPIError(msg)
        elif response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            try:
                msg = response.text
            except:
                msg = self.response_error_message(response)

            if response.status_code == 403:
                raise InvalidCredentialsError(msg)
            
            if response.status_code == 400 and 'INVALID_PARAMETER' in msg:
                try:
                    error_data = json.loads(msg)
                    error_message = error_data["errors"][0]["text"]
                    raise InvalidPayloadError(error_message)
                except:
                    raise InvalidPayloadError(msg)
            raise FatalAPIError(msg)

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ConnectionError, requests.exceptions.Timeout),
        max_tries=7,
        factor=2,
        jitter=lambda x: backoff.full_jitter(x) + x // 2
    )
    def request_api(self, method, request_data=None, endpoint=""):
        url = f"{self.base_url}{endpoint}"
        LOGGER.info(self.__auth)
        response = requests.request(
            method,
            url,
            json=request_data,
            auth=self.__auth,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        LOGGER.info(f"API Response: {response.status_code} - {response.text} - {response.request.headers}")
        self.validate_response(response)
        return response
