"""Tests for tap-concur."""

import datetime

import pytest
from hotglue_singer_sdk.testing import get_standard_tap_tests

from tap_concur.filters import (
    merge_digest_onto_invoice,
    parse_image_url_from_xml,
    parse_vendor_filter_selection,
    record_matches_vendor_filters,
)
from tap_concur.streams import AttachmentsStream, InvoicesStream
from tap_concur.tap import TapConcur

SAMPLE_CONFIG = {
    "client_id": "test-client",
    "client_secret": "test-secret",
    "refresh_token": "test-refresh",
    "api_url": "https://us2.api.concursolutions.com",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}

_STANDARD_TESTS = [
    t
    for t in get_standard_tap_tests(TapConcur, config=SAMPLE_CONFIG)
    if getattr(t, "__name__", "") != "_test_stream_connections"
]


@pytest.mark.parametrize("test_func", _STANDARD_TESTS)
def test_standard(test_func):
    """Run built-in SDK tap tests."""
    test_func()


def test_parse_image_url_unescapes_entities():
    xml = (
        '<Image xmlns="http://www.concursolutions.com/api/image/2011/02">'
        "<Url>https://example.com/file.pdf?id=1&amp;e=abc</Url>"
        "</Image>"
    )
    assert parse_image_url_from_xml(xml) == "https://example.com/file.pdf?id=1&e=abc"


def test_merge_digest_onto_invoice():
    digest = {
        "PaymentRequestId": "ABC123",
        "LastModifiedDate": "2023-03-23 12:48:43.0",
        "VendorCode": "V001",
        "VendorName": "Acme Corp",
    }
    detail = {"InvoiceNumber": "INV-1", "LineItems": []}
    merged = merge_digest_onto_invoice(digest, detail)
    assert merged["PaymentRequestId"] == "ABC123"
    assert merged["LastModifiedDate"] == "2023-03-23 12:48:43.0"
    assert merged["VendorCode"] == "V001"
    assert merged["InvoiceNumber"] == "INV-1"


def test_vendor_filter_by_code():
    record = {"VendorCode": "V001", "VendorName": "Acme"}
    assert record_matches_vendor_filters(record, {"V001"}, set()) is True
    assert record_matches_vendor_filters(record, {"V999"}, set()) is False


def test_vendor_filter_by_name():
    record = {"VendorCode": "V001", "VendorName": "Acme"}
    assert record_matches_vendor_filters(record, set(), {"Acme"}) is True


def test_parse_vendor_filter_selection():
    selected = {
        "vendor_code": ["V001", "V002"],
        "clauses": [{"field": "vendor_name", "values": ["Beta Inc"]}],
    }
    codes, names = parse_vendor_filter_selection(selected)
    assert codes == {"V001", "V002"}
    assert names == {"Beta Inc"}


def test_invoices_stream_has_child_context():
    tap = TapConcur(config=SAMPLE_CONFIG)
    stream = InvoicesStream(tap=tap)
    ctx = stream.get_child_context({"PaymentRequestId": "PR-1"})
    assert ctx == {"payment_request_id": "PR-1"}


def test_attachments_stream_is_child_of_invoices():
    assert AttachmentsStream.parent_stream_type is InvoicesStream
    assert AttachmentsStream.replication_key is None
