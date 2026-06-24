"""SAP Concur OAuth 2.0 authenticator."""

from __future__ import annotations

import json

import requests
from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from hotglue_singer_sdk.helpers._util import utc_now


class ConcurAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """OAuth authenticator for SAP Concur company-token flows."""

    @property
    def oauth_request_body(self) -> dict:
        """Return password (authtoken) or refresh_token grant body."""
        if self.config.get("refresh_token"):
            return {
                "grant_type": "refresh_token",
                "client_id": self.config["client_id"],
                "client_secret": self.config["client_secret"],
                "refresh_token": self.config["refresh_token"],
            }
        return {
            "grant_type": "password",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "username": self.config["username"],
            "password": self.config["password"],
            "credtype": self.config.get("credtype", "authtoken"),
        }

    def update_access_token_locally(self) -> None:
        """Refresh access token and persist rotated refresh token + geolocation."""
        request_time = utc_now()
        token_response = requests.post(
            self.auth_endpoint,
            data=self.oauth_request_payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            ) from ex

        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expires_in = token_json.get("expires_in", self._default_expiration)
        if expires_in is None:
            self.expires_in = None
        else:
            self.expires_in = int(expires_in) + int(request_time.timestamp())
        self.last_refreshed = request_time

        self._tap._config["access_token"] = token_json["access_token"]
        self._tap._config["expires_in"] = self.expires_in
        if token_json.get("refresh_token"):
            self._tap.logger.info("Latest refresh token: %s", token_json["refresh_token"])
            self._tap._config["refresh_token"] = token_json["refresh_token"]
        if token_json.get("geolocation"):
            self._tap._config["api_url"] = token_json["geolocation"]

        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w", encoding="utf-8") as outfile:
                json.dump(self._tap._config, outfile, indent=4)

    @classmethod
    def create_for_stream(cls, stream) -> ConcurAuthenticator:
        """Create a singleton authenticator for the stream's tap."""
        api_url = stream.config.get("api_url", "https://us.api.concursolutions.com").rstrip(
            "/"
        )
        config_file = getattr(stream._tap, "config_file", None)
        return cls(
            stream=stream,
            auth_endpoint=f"{api_url}/oauth2/v0/token",
            default_expiration=3600,
            config_file=config_file,
        )
