"""HTTP client and ConcurStream base class."""

from __future__ import annotations

from functools import cached_property
from typing import Any
from urllib.parse import urlparse

import requests
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
from hotglue_singer_sdk.streams import RESTStream
from typing_extensions import override

from tap_concur.auth import ConcurAuthenticator


class ConcurStream(RESTStream):
    """Base stream for SAP Concur REST APIs."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.NextPage"
    page_size = 1000

    @override
    @property
    def url_base(self) -> str:
        """API root from config (updated from token geolocation after auth)."""
        return self.config.get("api_url", "https://us.api.concursolutions.com").rstrip("/")

    @override
    @cached_property
    def authenticator(self) -> ConcurAuthenticator:
        """Return the shared OAuth authenticator."""
        return ConcurAuthenticator.create_for_stream(self)

    @override
    def prepare_request(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> requests.PreparedRequest:
        """Use full NextPage URLs when the API returns opaque pagination links."""
        if next_page_token and str(next_page_token).startswith("http"):
            return self.build_prepared_request(
                method=self.rest_method,
                url=str(next_page_token),
                params={},
                headers=self.http_headers,
            )
        return super().prepare_request(context, next_page_token)

    @override
    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return NextPage URL from list responses."""
        if self.next_page_token_jsonpath:
            try:
                body = response.json()
            except ValueError:
                return None
            matches = extract_jsonpath(self.next_page_token_jsonpath, body)
            next_page = next(iter(matches), None)
            if next_page:
                return next_page
        return response.headers.get("X-Next-Page")

    @override
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Default list pagination params (offset omitted on first page)."""
        params: dict[str, Any] = {}
        if next_page_token and not str(next_page_token).startswith("http"):
            params["offset"] = next_page_token
        if self.page_size:
            params["limit"] = self.page_size
        return params

    @override
    @property
    def http_headers(self) -> dict:
        headers = dict(super().http_headers)
        headers.setdefault("Accept", "application/json")
        return headers

    def _authenticated_get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """Perform an authenticated GET request."""
        headers = dict(self.http_headers)
        if extra_headers:
            headers.update(extra_headers)
        request = requests.Request("GET", url, params=params or {}, headers=headers)
        if self.authenticator:
            self.authenticator.authenticate_request(request)
        prepared = self.requests_session.prepare_request(request)
        response = self.requests_session.send(prepared, timeout=120)
        self.validate_response(response)
        return response

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict:
        """Authenticated GET returning parsed JSON."""
        url = f"{self.url_base}{path}"
        response = self._authenticated_get(url, params=params, extra_headers=headers)
        return response.json()

    @staticmethod
    def _filename_from_url(url: str, fallback: str) -> str:
        """Derive a filename from a URL path, ignoring query parameters."""
        try:
            segment = urlparse(url).path.rstrip("/").split("/")[-1]
        except (AttributeError, TypeError, ValueError):
            return f"{fallback}.pdf"
        if segment:
            return segment
        return f"{fallback}.pdf"
