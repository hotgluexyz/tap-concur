"""Concur tap class."""

from __future__ import annotations

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th
from typing_extensions import override

from tap_concur.auth import ConcurAuthenticator
from tap_concur.streams import AttachmentsStream, InvoicesStream, VendorsStream

STREAM_TYPES = [
    InvoicesStream,
    AttachmentsStream,
    VendorsStream,
]


class TapConcur(Tap):
    """Singer tap for SAP Concur Invoice."""

    name = "tap-concur"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="OAuth client ID from SAP Concur App Management",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="OAuth client secret",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            description="OAuth refresh token (rotated on each token refresh)",
        ),
        th.Property(
            "username",
            th.StringType,
            description="Company UUID for initial password/authtoken grant",
        ),
        th.Property(
            "password",
            th.StringType,
            description="Auth token (at-...) for initial password/authtoken grant",
        ),
        th.Property(
            "credtype",
            th.StringType,
            default="authtoken",
            description="Credential type for password grant (authtoken or password)",
        ),
        th.Property(
            "access_token",
            th.StringType,
            description="Optional cached access token (refreshed automatically)",
        ),
        th.Property(
            "api_url",
            th.StringType,
            description="API base URL (set from token geolocation after auth)",
            default="https://us.api.concursolutions.com",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest invoice LastModifiedDate to sync",
            default="2000-01-01T00:00:00Z",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @classmethod
    def access_token_support(cls, connector=None):
        """Support Hotglue --access-token CLI for token refresh."""
        config = connector.config if connector else {}
        api_url = config.get("api_url", "https://us.api.concursolutions.com").rstrip("/")
        return (ConcurAuthenticator, f"{api_url}/oauth2/v0/token")


if __name__ == "__main__":
    TapConcur.cli()
