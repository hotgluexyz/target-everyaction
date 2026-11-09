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
import re
import time
from email.utils import parsedate_to_datetime

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
        self._last_request_at = None
        self._retry_after_until = 0

    @property
    def base_url(self):
        return "https://api.securevan.com/v4/"

    def response_error_message(self, response: requests.Response) -> str:
        message = super().response_error_message(response)
        return re.sub(r"(/people/)\d+(?=/|$)", r"\1{personId}", message)

    @staticmethod
    def _retry_after_seconds(response: requests.Response) -> float:
        value = response.headers.get("Retry-After")
        if value is None:
            return 0
        try:
            return max(0, int(value))
        except ValueError:
            try:
                return max(0, parsedate_to_datetime(value).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                return 0

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
                    msg = error_data["errors"][0]["text"]
                except:
                    pass
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
        # Conservative per-key limits: 2 requests/sec for matching, 5 for others.
        path = endpoint.split("?", 1)[0].strip("/")
        interval = 0.5 if path in ("people/find", "people/findOrCreate") else 0.2
        next_request_at = self._retry_after_until
        if self._last_request_at is not None:
            next_request_at = max(next_request_at, self._last_request_at + interval)
        delay = next_request_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self._last_request_at = time.monotonic()

        response = requests.request(
            method,
            url,
            json=request_data,
            auth=self.__auth,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        LOGGER.info("API Response: %s for url: %s", response.status_code, response.url)
        if response.status_code == 429:
            timing_headers = {
                key: value for key, value in response.headers.items()
                if key.lower() == "retry-after"
                or key.lower().startswith(("ratelimit", "x-ratelimit", "x-rate-limit"))
            }
            LOGGER.warning("Rate limit response: %s - %s", response.text, timing_headers)
            self._retry_after_until = max(
                self._retry_after_until,
                time.monotonic() + self._retry_after_seconds(response),
            )
        self.validate_response(response)
        return response
