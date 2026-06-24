"""Stream type classes for tap-concur."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Optional

import requests
from hotglue_singer_sdk import typing as th
from typing_extensions import override

from tap_concur.client import ConcurStream
from tap_concur.filters import (
    merge_digest_onto_invoice,
    parse_image_url_from_xml,
    parse_vendor_filter_selection,
    record_matches_vendor_filters,
)

_ALLOCATION_SCHEMA = th.ObjectType(
    th.Property("Percentage", th.StringType),
    th.Property("Custom1", th.StringType),
    th.Property("Custom2", th.StringType),
    th.Property("Custom3", th.StringType),
    th.Property("Custom4", th.StringType),
    th.Property("Custom5", th.StringType),
    th.Property("Custom6", th.StringType),
    th.Property("Custom7", th.StringType),
    th.Property("Custom8", th.StringType),
    th.Property("Custom9", th.StringType),
    th.Property("Custom10", th.StringType),
)

_LINE_ITEM_SCHEMA = th.ObjectType(
    th.Property("Description", th.StringType),
    th.Property("ExpenseTypeCode", th.StringType),
    th.Property("ItemCode", th.StringType),
    th.Property("PurchaseOrderNumber", th.StringType),
    th.Property("Quantity", th.StringType),
    th.Property("UnitPrice", th.StringType),
    th.Property("TotalPrice", th.StringType),
    th.Property("Tax", th.StringType),
    th.Property("AmountWithoutVat", th.StringType),
    th.Property("VatAmount", th.StringType),
    th.Property("VatRate", th.StringType),
    th.Property("UnitOfMeasure", th.StringType),
    th.Property("SupplierPartId", th.StringType),
    th.Property(
        "Allocations",
        th.ArrayType(_ALLOCATION_SCHEMA),
    ),
)

_VENDOR_REMIT_SCHEMA = th.ObjectType(
    th.Property("VendorCode", th.StringType),
    th.Property("AddressCode", th.StringType),
    th.Property("Name", th.StringType),
    th.Property("Address1", th.StringType),
    th.Property("PostalCode", th.StringType),
)

_ADDRESS_SCHEMA = th.ObjectType(
    th.Property("Name", th.StringType),
    th.Property("AddressCode", th.StringType),
    th.Property("ExternalId", th.StringType),
    th.Property("VendorCode", th.StringType),
    th.Property("Address1", th.StringType),
    th.Property("Address2", th.StringType),
    th.Property("Address3", th.StringType),
    th.Property("City", th.StringType),
    th.Property("State", th.StringType),
    th.Property("PostalCode", th.StringType),
    th.Property("CountryCode", th.StringType),
    th.Property("DiscountTerms", th.StringType),
)

_STATUS_SCHEMA = th.ObjectType(
    th.Property("Code", th.StringType),
    th.Property("Message", th.StringType),
    th.Property("RecordNumber", th.StringType),
    th.Property("Type", th.StringType),
)

_VENDOR_BANK_SCHEMA = th.ObjectType(
    th.Property("AccountNumber", th.StringType),
    th.Property("AccountType", th.StringType),
    th.Property("AddressCode", th.StringType),
    th.Property("BankCode", th.StringType),
    th.Property("BankName", th.StringType),
    th.Property("BranchCode", th.StringType),
    th.Property("BranchLocation", th.StringType),
    th.Property("CountryCode", th.StringType),
    th.Property("CurrencyAlphaCode", th.StringType),
    th.Property("ID", th.StringType),
    th.Property("IsActive", th.StringType),
    th.Property("NameOnAccount", th.StringType),
    th.Property("RoutingNumber", th.StringType),
    th.Property("StatusList", th.ArrayType(_STATUS_SCHEMA)),
    th.Property("TransType", th.StringType),
    th.Property("URI", th.StringType),
    th.Property("VendorCode", th.StringType),
)


class InvoicesStream(ConcurStream):
    """Payment requests (invoices/bills) with nested line items."""

    name = "invoices"
    primary_keys = ["PaymentRequestId"]
    replication_key = "LastModifiedDate"
    replication_format = "%Y-%m-%d %H:%M:%S"
    records_jsonpath = "$.PaymentRequestDigest[*]"

    schema = th.PropertiesList(
        th.Property("PaymentRequestId", th.StringType),
        th.Property("LastModifiedDate", th.DateTimeType),
        th.Property("VendorCode", th.StringType),
        th.Property("VendorName", th.StringType),
        th.Property("InvoiceNumber", th.StringType),
        th.Property("ApprovalStatusCode", th.StringType),
        th.Property("PaymentStatusCode", th.StringType),
        th.Property("IsDeleted", th.StringType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("CountryCode", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("InvoiceAmount", th.StringType),
        th.Property("InvoiceDate", th.StringType),
        th.Property("InvoiceReceivedDate", th.StringType),
        th.Property("PaymentAmount", th.StringType),
        th.Property("PaymentDueDate", th.StringType),
        th.Property("PurchaseOrderNumber", th.StringType),
        th.Property("LedgerCode", th.StringType),
        th.Property("EmployeeName", th.StringType),
        th.Property("DataSource", th.StringType),
        th.Property("OB10TransactionId", th.StringType),
        th.Property("OB10BuyerId", th.StringType),
        th.Property("BuyerCostCenter", th.StringType),
        th.Property("CheckNumber", th.StringType),
        th.Property("PaymentTermsDays", th.StringType),
        th.Property("PaymentMethod", th.StringType),
        th.Property("PaymentAdjustmentNotes", th.StringType),
        th.Property("ReceiptConfirmationType", th.StringType),
        th.Property("IsInvoiceConfirmed", th.StringType),
        th.Property("NotesToVendor", th.StringType),
        th.Property("DiscountTerms", th.StringType),
        th.Property("ApprovalStatus", th.StringType),
        th.Property("SubmittedByDelegate", th.StringType),
        th.Property("ApprovedByDelegate", th.StringType),
        th.Property("IsAssigned", th.StringType),
        th.Property("CreatedByUsername", th.StringType),
        th.Property("AssignedByUsername", th.StringType),
        th.Property("PaymentRequestCreatedByTestUser", th.StringType),
        th.Property("PaymentRequestDeletedBy", th.StringType),
        th.Property("IsPaymentRequestDeleted", th.StringType),
        th.Property("IsTestTransaction", th.StringType),
        th.Property("IsPaymentRequestDuplicate", th.StringType),
        th.Property("UserCreationDate", th.StringType),
        th.Property("PostingDate", th.StringType),
        th.Property("FirstSubmitDate", th.StringType),
        th.Property("LastSubmitDate", th.StringType),
        th.Property("WorkflowCompleteDate", th.StringType),
        th.Property("ExtractDate", th.StringType),
        th.Property("PaidDate", th.StringType),
        th.Property("FirstApprovalDate", th.StringType),
        th.Property("AssignedDate", th.StringType),
        th.Property("DeletedDate", th.StringType),
        th.Property("PaymentStatus", th.StringType),
        th.Property("CalculatedAmount", th.StringType),
        th.Property("TotalApprovedAmount", th.StringType),
        th.Property("ShippingAmount", th.StringType),
        th.Property("TaxAmount", th.StringType),
        th.Property("LineItemTotalAmount", th.StringType),
        th.Property("PaidAmount", th.StringType),
        th.Property("DiscountPercentage", th.StringType),
        th.Property("LineItemVatAmount", th.StringType),
        th.Property("DeliverySlipNumber", th.StringType),
        th.Property("OrgUnit1", th.StringType),
        th.Property("OrgUnit2", th.StringType),
        th.Property("OrgUnit3", th.StringType),
        th.Property("OrgUnit4", th.StringType),
        th.Property("OrgUnit5", th.StringType),
        th.Property("OrgUnit6", th.StringType),
        th.Property("Custom1", th.StringType),
        th.Property("Custom2", th.StringType),
        th.Property("Custom3", th.StringType),
        th.Property("Custom4", th.StringType),
        th.Property("Custom5", th.StringType),
        th.Property("Custom6", th.StringType),
        th.Property("Custom7", th.StringType),
        th.Property("Custom8", th.StringType),
        th.Property("Custom9", th.StringType),
        th.Property("Custom10", th.StringType),
        th.Property("Custom11", th.StringType),
        th.Property("Custom12", th.StringType),
        th.Property("Custom13", th.StringType),
        th.Property("Custom14", th.StringType),
        th.Property("Custom15", th.StringType),
        th.Property("Custom16", th.StringType),
        th.Property("Custom17", th.StringType),
        th.Property("Custom18", th.StringType),
        th.Property("Custom19", th.StringType),
        th.Property("Custom20", th.StringType),
        th.Property("Custom21", th.StringType),
        th.Property("Custom22", th.StringType),
        th.Property("Custom23", th.StringType),
        th.Property("Custom24", th.StringType),
        th.Property("AmountWithoutVat", th.StringType),
        th.Property("VatAmountOne", th.StringType),
        th.Property("VatAmountTwo", th.StringType),
        th.Property("VatAmountThree", th.StringType),
        th.Property("VatAmountFour", th.StringType),
        th.Property("VatRateOne", th.StringType),
        th.Property("VatRateTwo", th.StringType),
        th.Property("VatRateThree", th.StringType),
        th.Property("VatRateFour", th.StringType),
        th.Property("TaxCode", th.StringType),
        th.Property("ProvincialTaxId", th.StringType),
        th.Property("VendorTaxId", th.StringType),
        th.Property("ExternalPolicyId", th.StringType),
        th.Property("TaxCode2", th.StringType),
        th.Property("TaxCode3", th.StringType),
        th.Property("TaxCode4", th.StringType),
        th.Property("VendorRemitToIdentifier", _VENDOR_REMIT_SCHEMA),
        th.Property("VendorRemitAddress", _ADDRESS_SCHEMA),
        th.Property("VendorShipFromAddress", _ADDRESS_SCHEMA),
        th.Property("CompanyBillToAddress", _ADDRESS_SCHEMA),
        th.Property("CompanyShipToAddress", _ADDRESS_SCHEMA),
        th.Property("ID", th.StringType),
        th.Property("URI", th.StringType),
        th.Property("LineItems", th.ArrayType(_LINE_ITEM_SCHEMA)),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._vendor_codes: set[str] = set()
        self._vendor_names: set[str] = set()
        self._earliest_failed_modified: str | None = None

    @override
    def setup_selected_filters(self) -> None:
        self._vendor_codes, self._vendor_names = parse_vendor_filter_selection(
            self._selected_filters
        )

    @override
    def get_child_context(
        self,
        record: dict,
        context: Optional[dict] = None,
    ) -> dict:
        return {"payment_request_id": record["PaymentRequestId"]}

    def get_available_filters_metadata(self) -> dict[str, Any]:
        return {
            "supported_operators": ["AND", "OR"],
            "supports_nesting_clauses": True,
            "filters": {
                "vendor_code": {
                    "label": "Vendor Code",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "VendorCode",
                    "options": "reference_data.vendors.VendorCode",
                },
                "vendor_name": {
                    "label": "Vendor Name",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "VendorName",
                    "options": "reference_data.vendors.VendorName",
                },
            },
        }

    def _incremental_filter_date(self, context: dict | None) -> str:
        bookmark = self.get_starting_replication_key_value(context)
        if bookmark:
            return str(bookmark)[:10]
        start = self.config.get("start_date", "2000-01-01T00:00:00Z")
        return str(start)[:10]

    def _register_failed_invoice(self, digest: dict, context: dict | None) -> None:
        """Cap the bookmark so a failed invoice is retried on the next run.

        When a detail fetch fails the invoice is never emitted, but other invoices
        with a newer ``LastModifiedDate`` would otherwise advance the bookmark past
        it (the SDK promotes the max replication key value of emitted records). We
        lower the replication-key signpost to the earliest failed invoice's
        ``LastModifiedDate`` so ``finalize_state_progress_markers`` clamps the
        bookmark and the next run's ``lastModifiedDateAfter`` re-includes it.
        """
        failed_modified = digest.get("LastModifiedDate")
        if not failed_modified:
            return
        failed_modified = str(failed_modified)
        if (
            self._earliest_failed_modified is None
            or failed_modified < self._earliest_failed_modified
        ):
            self._earliest_failed_modified = failed_modified
            self._write_replication_key_signpost(context, self._earliest_failed_modified)

    @override
    def get_records(self, context: dict | None) -> Iterable[dict]:
        """List digests incrementally, then fetch full payment request per ID."""
        modified_after = self._incremental_filter_date(context)
        self._earliest_failed_modified = None
        next_page_token: Any | None = None

        while True:
            if next_page_token and str(next_page_token).startswith("http"):
                response = self._authenticated_get(
                    str(next_page_token),
                    extra_headers={"Accept": "application/json"},
                )
            else:
                params: dict[str, Any] = {
                    "lastModifiedDateAfter": modified_after,
                    "limit": self.page_size,
                }
                response = self._authenticated_get(
                    f"{self.url_base}/api/v3.0/invoice/paymentrequestdigests",
                    params=params,
                    extra_headers={"Accept": "application/json"},
                )
            body = response.json()
            digests = body.get("PaymentRequestDigest") or []

            for digest in digests:
                payment_request_id = digest.get("PaymentRequestId") or digest.get("ID")
                if not payment_request_id:
                    continue
                try:
                    detail = self._get_json(
                        f"/api/v3.0/invoice/paymentrequest/{payment_request_id}",
                        headers={"Accept": "application/json"},
                    )
                except Exception as ex:
                    self.logger.warning(
                        "Failed to fetch payment request %s: %s",
                        payment_request_id,
                        ex,
                    )
                    self._register_failed_invoice(digest, context)
                    continue

                record = merge_digest_onto_invoice(digest, detail)
                if not record_matches_vendor_filters(
                    record, self._vendor_codes, self._vendor_names
                ):
                    continue
                yield record

            next_page_token = body.get("NextPage")
            if not next_page_token:
                break


class AttachmentsStream(ConcurStream):
    """Invoice image attachments — child of invoices."""

    name = "attachments"
    parent_stream_type = InvoicesStream
    primary_keys = ["payment_request_id", "file_name"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("payment_request_id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("file_name", th.StringType),
        th.Property("file_path", th.StringType),
        th.Property(
            "download_status",
            th.StringType,
            description="success, not_found, or error",
        ),
        th.Property("error_message", th.StringType),
    ).to_dict()

    @override
    def get_context_state(self, context: Optional[dict]) -> dict:
        return self.stream_state

    def _output_folder(self, payment_request_id: str) -> Path:
        job_id = os.environ.get("JOB_ID")
        base = Path(f"/home/hotglue/{job_id}/sync-output") if job_id else Path(".")
        folder = base / "attachments" / payment_request_id
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _attachment_filename(
        self,
        payment_request_id: str,
        content_type: str | None = None,
    ) -> str:
        """Build a short, stable filename for Concur image downloads."""
        ext = ".pdf"
        if content_type:
            lowered = content_type.lower()
            if "png" in lowered:
                ext = ".png"
            elif "jpeg" in lowered or "jpg" in lowered:
                ext = ".jpg"
        return f"{payment_request_id}{ext}"

    def _download_file(self, payment_request_id: str, url: str) -> dict:
        try:
            response = requests.get(url, timeout=30)
            file_name = self._attachment_filename(
                payment_request_id,
                response.headers.get("Content-Type"),
            )
            file_path = self._output_folder(payment_request_id) / file_name
            if response.status_code == 404:
                return {
                    "payment_request_id": payment_request_id,
                    "url": url,
                    "file_name": file_name,
                    "file_path": None,
                    "download_status": "not_found",
                    "error_message": None,
                }
            if response.status_code >= 400:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                self.logger.warning(
                    "Failed to download attachment for %s: %s",
                    payment_request_id,
                    error_msg,
                )
                return {
                    "payment_request_id": payment_request_id,
                    "url": url,
                    "file_name": file_name,
                    "file_path": None,
                    "download_status": "error",
                    "error_message": error_msg,
                }
            with open(file_path, "wb") as outfile:
                outfile.write(response.content)
            self.logger.info(
                "Downloaded attachment for %s → %s",
                payment_request_id,
                file_path,
            )
            return {
                "payment_request_id": payment_request_id,
                "url": url,
                "file_name": file_name,
                "file_path": str(file_path),
                "download_status": "success",
                "error_message": None,
            }
        except Exception as ex:
            self.logger.error(
                "Error downloading attachment for %s: %s",
                payment_request_id,
                ex,
            )
            return {
                "payment_request_id": payment_request_id,
                "url": url,
                "file_name": self._attachment_filename(payment_request_id),
                "file_path": None,
                "download_status": "error",
                "error_message": str(ex),
            }

    @override
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        payment_request_id = context["payment_request_id"]
        image_url = f"{self.url_base}/api/image/v1.0/invoice/{payment_request_id}"

        request = requests.Request(
            "GET",
            image_url,
            headers={**self.http_headers, "Accept": "application/xml"},
        )
        if self.authenticator:
            self.authenticator.authenticate_request(request)
        prepared = self.requests_session.prepare_request(request)
        response = self.requests_session.send(prepared, timeout=120)

        if response.status_code == 404:
            yield {
                "payment_request_id": payment_request_id,
                "url": None,
                "file_name": None,
                "file_path": None,
                "download_status": "not_found",
                "error_message": None,
            }
            return

        self.validate_response(response)
        image_url = parse_image_url_from_xml(response.text)
        if not image_url:
            yield {
                "payment_request_id": payment_request_id,
                "url": None,
                "file_name": None,
                "file_path": None,
                "download_status": "not_found",
                "error_message": "No Url in image response",
            }
            return

        yield self._download_file(payment_request_id, image_url)


class VendorsStream(ConcurStream):
    """Invoice vendors / suppliers."""

    name = "vendors"
    path = "/api/v3.1/invoice/vendors"
    primary_keys = ["ID"]
    replication_key = None
    records_jsonpath = "$.Vendor[*]"

    schema = th.PropertiesList(
        th.Property("ID", th.StringType),
        th.Property("AccountNumber", th.StringType),
        th.Property("VendorCode", th.StringType),
        th.Property("VendorName", th.StringType),
        th.Property("VendorFormName", th.StringType),
        th.Property("AddressCode", th.StringType),
        th.Property("AddressImportSyncID", th.StringType),
        th.Property("Address1", th.StringType),
        th.Property("Address2", th.StringType),
        th.Property("Address3", th.StringType),
        th.Property("City", th.StringType),
        th.Property("State", th.StringType),
        th.Property("PostalCode", th.StringType),
        th.Property("Country", th.StringType),
        th.Property("CountryCode", th.StringType),
        th.Property("TaxID", th.StringType),
        th.Property("TaxType", th.StringType),
        th.Property("ProvincialTaxID", th.StringType),
        th.Property("PaymentMethodType", th.StringType),
        th.Property("PaymentTerms", th.StringType),
        th.Property("DiscountTermsDays", th.StringType),
        th.Property("DiscountPercentage", th.StringType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("Approved", th.StringType),
        th.Property("ContactEmail", th.StringType),
        th.Property("ContactFirstName", th.StringType),
        th.Property("ContactLastName", th.StringType),
        th.Property("ContactPhoneNumber", th.StringType),
        th.Property("PurchaseOrderContactEmail", th.StringType),
        th.Property("PurchaseOrderContactFirstName", th.StringType),
        th.Property("PurchaseOrderContactLastName", th.StringType),
        th.Property("PurchaseOrderContactPhoneNumber", th.StringType),
        th.Property("DefaultEmployeeID", th.StringType),
        th.Property("DefaultExpenseTypeName", th.StringType),
        th.Property("ShippingMethod", th.StringType),
        th.Property("ShippingTerms", th.StringType),
        th.Property("IsVisibleForContentExtraction", th.StringType),
        th.Property("IsLineItemVatIncld", th.StringType),
        th.Property("VoucherNotes", th.StringType),
        th.Property("Custom1", th.StringType),
        th.Property("Custom2", th.StringType),
        th.Property("Custom3", th.StringType),
        th.Property("Custom4", th.StringType),
        th.Property("Custom5", th.StringType),
        th.Property("Custom6", th.StringType),
        th.Property("Custom7", th.StringType),
        th.Property("Custom8", th.StringType),
        th.Property("Custom9", th.StringType),
        th.Property("Custom10", th.StringType),
        th.Property("Custom11", th.StringType),
        th.Property("Custom12", th.StringType),
        th.Property("Custom13", th.StringType),
        th.Property("Custom14", th.StringType),
        th.Property("Custom15", th.StringType),
        th.Property("Custom16", th.StringType),
        th.Property("Custom17", th.StringType),
        th.Property("Custom18", th.StringType),
        th.Property("Custom19", th.StringType),
        th.Property("Custom20", th.StringType),
        th.Property("StatusList", th.ArrayType(_STATUS_SCHEMA)),
        th.Property("VendorBankList", th.ArrayType(_VENDOR_BANK_SCHEMA)),
        th.Property("VendorGroupList", th.ArrayType(th.StringType)),
        th.Property("URI", th.StringType),
    ).to_dict()

    def get_available_filters_metadata(self) -> dict[str, Any]:
        return {
            "supported_operators": ["AND", "OR"],
            "supports_nesting_clauses": True,
            "filters": {
                "vendor_code": {
                    "label": "Vendor Code",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "VendorCode",
                    "options": "reference_data.vendors.VendorCode",
                },
                "vendor_name": {
                    "label": "Vendor Name",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "VendorName",
                    "options": "reference_data.vendors.VendorName",
                },
            },
        }
