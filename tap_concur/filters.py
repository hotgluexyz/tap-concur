"""Vendor filter helpers for invoice stream."""

from __future__ import annotations

from typing import Any


def parse_vendor_filter_selection(selected_filters: dict | None) -> tuple[set[str], set[str]]:
    """Extract vendor code/name allow-lists from Hotglue selected filter config."""
    vendor_codes: set[str] = set()
    vendor_names: set[str] = set()
    if not selected_filters:
        return vendor_codes, vendor_names

    for key in ("vendor_code", "vendor_id"):
        values = selected_filters.get(key)
        if values:
            vendor_codes.update(_as_list(values))

    vendor_name_values = selected_filters.get("vendor_name")
    if vendor_name_values:
        vendor_names.update(_as_list(vendor_name_values))

    for clause in selected_filters.get("clauses", []):
        field = clause.get("field") or clause.get("filter_name") or clause.get("name")
        values = clause.get("values") or clause.get("value")
        if field in ("vendor_code", "vendor_id"):
            vendor_codes.update(_as_list(values))
        elif field == "vendor_name":
            vendor_names.update(_as_list(values))

    return vendor_codes, vendor_names


def record_matches_vendor_filters(
    record: dict[str, Any],
    vendor_codes: set[str],
    vendor_names: set[str],
) -> bool:
    """Return True when the record passes configured vendor filters (or none set)."""
    if not vendor_codes and not vendor_names:
        return True

    code = record.get("VendorCode")
    name = record.get("VendorName")
    remit = record.get("VendorRemitToIdentifier") or {}
    if not code:
        code = remit.get("VendorCode")
    if not name:
        name = remit.get("Name")

    if vendor_codes and code in vendor_codes:
        return True
    if vendor_names and name in vendor_names:
        return True
    return False


def merge_digest_onto_invoice(digest: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    """Attach digest metadata required for replication and filtering onto invoice detail."""
    merged = dict(detail)
    merged["PaymentRequestId"] = digest.get("PaymentRequestId") or digest.get("ID")
    if digest.get("LastModifiedDate"):
        merged["LastModifiedDate"] = digest["LastModifiedDate"]
    for field in (
        "VendorCode",
        "VendorName",
        "InvoiceNumber",
        "ApprovalStatusCode",
        "PaymentStatusCode",
        "IsDeleted",
    ):
        if digest.get(field) is not None and merged.get(field) is None:
            merged[field] = digest[field]

    line_items = merged.get("LineItems")
    if isinstance(line_items, dict) and "LineItem" in line_items:
        merged["LineItems"] = line_items["LineItem"]

    remit = merged.get("VendorRemitToIdentifier")
    if not remit and merged.get("VendorRemitAddress"):
        addr = merged["VendorRemitAddress"]
        merged["VendorRemitToIdentifier"] = {
            "VendorCode": addr.get("VendorCode"),
            "AddressCode": addr.get("AddressCode"),
            "Name": addr.get("Name"),
            "Address1": addr.get("Address1"),
            "PostalCode": addr.get("PostalCode"),
        }

    return merged


def parse_image_url_from_xml(xml_text: str) -> str | None:
    """Extract and unescape the image download URL from Concur Image API XML."""
    import html
    import re

    match = re.search(r"<Url>(.*?)</Url>", xml_text, re.DOTALL)
    if not match:
        return None
    return html.unescape(match.group(1).strip())


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]
