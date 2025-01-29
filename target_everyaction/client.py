from target_hotglue.client import HotglueSink
import requests
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
import singer
from target_everyaction.auth import EveryActionAuth
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

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
            raise FatalAPIError(msg)

    def request_api(self, method, endpoint, request_data=None, params=None):
        url = f"{self.base_url}{endpoint}"
        LOGGER.info(self.__auth)
        response = requests.request(
            method,
            url,
            json=request_data,
            params=params,
            auth=self.__auth,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        LOGGER.info(f"API Response: {response.status_code} - {response.text} - {response.request.headers}")
        self.validate_response(response)
        return response
