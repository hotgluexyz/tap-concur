# tap-concur

Singer tap for syncing **invoices**, **attachments**, and **vendors** from the [SAP Concur Invoice API](https://developer.concur.com/api-reference/invoice/v3.payment-request.html).

Built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK).

## Streams

| Stream | Description | Replication |
|--------|-------------|-------------|
| `invoices` | Payment requests (invoices/bills) with nested line items and allocations | Incremental on `LastModifiedDate` |
| `attachments` | Invoice PDF/image files (child of `invoices`) | Full refresh per parent invoice |
| `vendors` | Vendor/supplier master data | Full table |

### Parent-child behavior

`attachments` is a **child stream** of `invoices`. For each invoice synced, the tap fetches a short-lived image URL from the Concur Image API and downloads the binary immediately. See [Hotglue parent-child streams](https://docs.hotglue.com/custom-connectors/connector-builder#parent-child-streams).

Attachment files are written to:

- **hotglue runtime:** `/home/hotglue/{JOB_ID}/sync-output/attachments/{payment_request_id}/`
- **local dev:** `./attachments/{payment_request_id}/`

## Authentication

SAP Concur uses OAuth 2.0 company-token flow with a **1-hour access token** and **6-month refresh token**. Each refresh returns a **new refresh token** — the tap persists it to your config file. **Do not lose the latest refresh token** or the customer must re-authenticate.

### Initial token (password grant, `credtype=authtoken`)

```bash
curl -X POST 'https://us.api.concursolutions.com/oauth2/v0/token' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'client_id=YOUR_CLIENT_ID' \
  -d 'client_secret=YOUR_CLIENT_SECRET' \
  -d 'grant_type=password' \
  -d 'username=YOUR_COMPANY_UUID' \
  -d 'credtype=authtoken' \
  -d 'password=at-YOUR_AUTH_TOKEN'
```

Save `access_token`, `refresh_token`, and `geolocation` from the response. Use `geolocation` as `api_url`.

### Runtime token refresh

```bash
curl -X POST 'https://us.api.concursolutions.com/oauth2/v0/token' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'client_id=YOUR_CLIENT_ID' \
  -d 'client_secret=YOUR_CLIENT_SECRET' \
  -d 'grant_type=refresh_token' \
  -d 'refresh_token=YOUR_REFRESH_TOKEN'
```

Or use the tap CLI:

```bash
tap-concur --config .secrets/config.json --access-token
```

## Configuration

| Setting | Required | Description |
|---------|----------|-------------|
| `client_id` | Yes | OAuth client ID |
| `client_secret` | Yes | OAuth client secret |
| `refresh_token` | Yes (after initial auth) | Refresh token (rotated on each sync) |
| `username` | Initial auth only | Company UUID |
| `password` | Initial auth only | `at-...` auth token |
| `api_url` | No | API base URL (default US; use `geolocation` from token response) |
| `start_date` | No | Earliest `LastModifiedDate` for invoice digests |

Example `.secrets/config.json`:

```json
{
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "refresh_token": "your-refresh-token",
  "api_url": "https://us2.api.concursolutions.com",
  "start_date": "2020-01-01T00:00:00Z"
}
```

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Usage

```bash
# Discover streams
tap-concur --config .secrets/config.json --discover > catalog.json

# Sync all streams
tap-concur --config .secrets/config.json --catalog catalog.json --state .secrets/state.json

# Available vendor filters for invoices
tap-concur --config .secrets/config.json --catalog catalog.json --get-available-filters
```

## API endpoints

| Step | Endpoint |
|------|----------|
| Token | `POST /oauth2/v0/token` |
| Invoice digests | `GET /api/v3.0/invoice/paymentrequestdigests?lastModifiedDateAfter={date}` |
| Invoice detail | `GET /api/v3.0/invoice/paymentrequest/{id}` |
| Vendors | `GET /api/v3.1/invoice/vendors` |
| Image URL | `GET /api/image/v1.0/invoice/{requestId}` |
| Download | `GET {Url}` (15-minute TTL, no auth) |

## Vendor filtering

The `invoices` stream supports filtering by vendor code or vendor name using Hotglue available filters. The `vendors` stream provides reference data for the filter picker.

## Development

```bash
pytest
ruff check .
```

## References

- [Concur Authentication](https://developer.concur.com/api-reference/authentication/apidoc.html)
- [Payment Request Digests](https://developer.concur.com/api-reference/invoice/v3.payment-request-digest.html)
- [Payment Request](https://developer.concur.com/api-reference/invoice/v3.payment-request.html)
- [Vendor v3.1](https://developer.concur.com/api-reference/invoice/v3.1.vendor.html)
- [Image v1](https://developer.concur.com/api-reference/image/v1.image.html)
