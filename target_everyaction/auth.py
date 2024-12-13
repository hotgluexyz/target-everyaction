import base64
import requests


class EveryActionAuth(requests.auth.AuthBase):
    def __init__(self, username, password):
        self.username = username

        if not password.endswith("|0") and not password.endswith("|1"):
            password += "|0"

        self.password = password

    def __call__(self, r):
        r.headers['Authorization'] = requests.auth._basic_auth_str(self.username, self.password)
        return r
