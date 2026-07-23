"""Tests for tap-concur."""

import datetime
from unittest.mock import MagicMock, patch

import pytest
from hotglue_singer_sdk.testing import get_standard_tap_tests

from tap_concur.filters import (
    merge_digest_onto_invoice,
    parse_image_url_from_xml,
    parse_vendor_filter_selection,
    record_matches_vendor_filters,
)
from tap_concur.streams import AttachmentsStream, InvoicesStream, VendorsStream
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
        "clause_1": {
            "field": "vendor_code",
            "operator": "IN",
            "value": ["V001", "V002"],
        },
        "clause_2": {
            "field": "vendor_name",
            "operator": "EQ",
            "value": "Beta Inc",
        },
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


def test_invoices_get_records_filters_by_vendor_name_before_detail_fetch():
    """VendorName filter should keep matching digests and skip detail for others."""
    tap = TapConcur(config=SAMPLE_CONFIG)
    stream = InvoicesStream(tap=tap)
    stream._selected_filters = {
        "clause_1": {
            "field": "vendor_name",
            "operator": "IN",
            "value": ["Acme Corp"],
        }
    }
    stream.setup_selected_filters()

    digests = {
        "PaymentRequestDigest": [
            {
                "PaymentRequestId": "PR-KEEP",
                "LastModifiedDate": "2024-01-02 10:00:00.0",
                "VendorCode": "V001",
                "VendorName": "Acme Corp",
            },
            {
                "PaymentRequestId": "PR-SKIP",
                "LastModifiedDate": "2024-01-03 10:00:00.0",
                "VendorCode": "V002",
                "VendorName": "Other Vendor",
            },
        ],
        "NextPage": None,
    }
    digest_response = MagicMock()
    digest_response.json.return_value = digests

    detail = {
        "InvoiceNumber": "INV-KEEP",
        "LineItems": [],
    }

    with (
        patch.object(stream, "_authenticated_get", return_value=digest_response) as mock_get,
        patch.object(stream, "_get_json", return_value=detail) as mock_detail,
    ):
        records = list(stream.get_records(context=None))

    assert len(records) == 1
    assert records[0]["PaymentRequestId"] == "PR-KEEP"
    assert records[0]["VendorName"] == "Acme Corp"
    assert records[0]["InvoiceNumber"] == "INV-KEEP"

    mock_get.assert_called_once()
    mock_detail.assert_called_once_with(
        "/api/v3.0/invoice/paymentrequest/PR-KEEP",
        headers={"Accept": "application/json"},
    )


def test_invoices_get_records_no_vendor_filter_fetches_all_details():
    """Without vendor filters, every digest should trigger a detail fetch."""
    tap = TapConcur(config=SAMPLE_CONFIG)
    stream = InvoicesStream(tap=tap)
    stream._selected_filters = {}
    stream.setup_selected_filters()

    digests = {
        "PaymentRequestDigest": [
            {
                "PaymentRequestId": "PR-1",
                "LastModifiedDate": "2024-01-02 10:00:00.0",
                "VendorName": "Acme Corp",
            },
            {
                "PaymentRequestId": "PR-2",
                "LastModifiedDate": "2024-01-03 10:00:00.0",
                "VendorName": "Other Vendor",
            },
        ],
        "NextPage": None,
    }
    digest_response = MagicMock()
    digest_response.json.return_value = digests

    def _detail_for(path: str, **_kwargs):
        payment_request_id = path.rsplit("/", 1)[-1]
        return {"InvoiceNumber": f"INV-{payment_request_id}", "LineItems": []}

    with (
        patch.object(stream, "_authenticated_get", return_value=digest_response),
        patch.object(stream, "_get_json", side_effect=_detail_for) as mock_detail,
    ):
        records = list(stream.get_records(context=None))

    assert {r["PaymentRequestId"] for r in records} == {"PR-1", "PR-2"}
    assert mock_detail.call_count == 2


def test_vendors_get_records_pages_full_list_without_filters():
    tap = TapConcur(config=SAMPLE_CONFIG)
    stream = VendorsStream(tap=tap)
    stream._selected_filters = {}
    stream.setup_selected_filters()

    page1 = MagicMock()
    page1.json.return_value = {
        "Vendor": [{"ID": "1", "VendorCode": "V001", "VendorName": "Acme"}],
        "NextPage": "https://us2.api.concursolutions.com/api/v3.1/invoice/vendors?offset=abc",
    }
    page2 = MagicMock()
    page2.json.return_value = {
        "Vendor": [{"ID": "2", "VendorCode": "V002", "VendorName": "Beta"}],
        "NextPage": None,
    }

    with patch.object(stream, "_authenticated_get", side_effect=[page1, page2]) as mock_get:
        records = list(stream.get_records(context=None))

    assert [r["ID"] for r in records] == ["1", "2"]
    assert mock_get.call_count == 2
    first_call_url = mock_get.call_args_list[0].args[0]
    assert first_call_url.endswith("/api/v3.1/invoice/vendors")
    assert mock_get.call_args_list[0].kwargs["params"]["limit"] == stream.page_size


def test_vendors_pagination_applies_bare_offset_cursor():
    """Concur may return NextPage as an offset string, not a full URL."""
    tap = TapConcur(config=SAMPLE_CONFIG)
    stream = VendorsStream(tap=tap)
    stream._selected_filters = {}
    stream.setup_selected_filters()

    page1 = MagicMock()
    page1.json.return_value = {
        "Vendor": [{"ID": "1", "VendorCode": "V001", "VendorName": "Acme"}],
        "NextPage": "offset-token-1",
    }
    page2 = MagicMock()
    page2.json.return_value = {
        "Vendor": [{"ID": "2", "VendorCode": "V002", "VendorName": "Beta"}],
        "NextPage": None,
    }

    with patch.object(stream, "_authenticated_get", side_effect=[page1, page2]) as mock_get:
        records = list(stream.get_records(context=None))

    assert [r["ID"] for r in records] == ["1", "2"]
    assert mock_get.call_count == 2
    assert mock_get.call_args_list[1].kwargs["params"]["offset"] == "offset-token-1"


def test_vendors_pagination_stops_on_repeated_next_page():
    """Stop when Concur returns the same NextPage cursor repeatedly."""
    tap = TapConcur(config=SAMPLE_CONFIG)
    stream = VendorsStream(tap=tap)
    stream._selected_filters = {}
    stream.setup_selected_filters()

    stuck_url = "https://us2.api.concursolutions.com/api/v3.1/invoice/vendors?offset=stuck"
    page1 = MagicMock()
    page1.json.return_value = {
        "Vendor": [{"ID": "1", "VendorCode": "V001", "VendorName": "Acme"}],
        "NextPage": stuck_url,
    }
    page2 = MagicMock()
    page2.json.return_value = {
        "Vendor": [{"ID": "1", "VendorCode": "V001", "VendorName": "Acme"}],
        "NextPage": stuck_url,
    }

    with patch.object(stream, "_authenticated_get", side_effect=[page1, page2]) as mock_get:
        records = list(stream.get_records(context=None))

    assert [r["ID"] for r in records] == ["1", "1"]
    assert mock_get.call_count == 2


def test_vendors_get_records_uses_api_search_for_vendor_filters():
    tap = TapConcur(config=SAMPLE_CONFIG)
    stream = VendorsStream(tap=tap)
    stream._selected_filters = {
        "clause_1": {
            "field": "vendor_name",
            "operator": "IN",
            "value": ["Acme Corp"],
        }
    }
    stream.setup_selected_filters()

    response = MagicMock()
    response.json.return_value = {
        "Vendor": [
            {"ID": "1", "VendorCode": "V001", "VendorName": "Acme Corp"},
            {"ID": "2", "VendorCode": "V002", "VendorName": "Other"},
        ],
        "NextPage": None,
    }

    with patch.object(stream, "_authenticated_get", return_value=response) as mock_get:
        records = list(stream.get_records(context=None))

    assert len(records) == 1
    assert records[0]["VendorName"] == "Acme Corp"
    params = mock_get.call_args.kwargs["params"]
    assert params["vendorName"] == "Acme Corp"
    assert params["searchType"] == "exact"
