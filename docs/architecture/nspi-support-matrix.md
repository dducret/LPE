# NSPI Support Matrix

## Current State/Functionality Overview

`NSPI` is an authenticated address-book compatibility surface for Outlook profile
creation and address resolution. It projects canonical `LPE` tenant directory
accounts and readable canonical contacts. It does not own directory state and
does not mutate address-book data.

The Microsoft `MS-NSPI` reference checked for this matrix on 2026-05-14 was the
published `16.0` version dated 2024-04-23.

## Implementation/Usage

- `POST /mapi/nspi` uses MAPI/HTTP request headers and session cookies.
- `Bind` creates an authenticated NSPI session bound to the tenant, account, and
  mailbox identity in the authenticated principal.
- Established NSPI operations require the bound session cookie and reject a
  changed authentication context.
- Directory rows are loaded through a tenant-explicit address-book store API.
- Tenant directory accounts are limited to active accounts with tenant GAL
  visibility.
- Contact rows come only from canonical contacts readable by the authenticated
  account.
- Hidden authenticated accounts are not browsed, but can resolve themselves for
  Outlook profile bootstrap.
- `GetMailboxUrl` and `GetAddressBookUrl` return the real public MAPI/HTTP
  endpoint URLs derived from request host/proxy headers.

## Reference Table/List

| NSPI method | LPE behavior | Storage boundary | Status |
| --- | --- | --- | --- |
| `Bind` | Creates or reconnects an authenticated NSPI session and returns the server GUID. | Session state only. | Supported |
| `Unbind` | Releases the authenticated NSPI session. | Session state only. | Supported |
| `QueryRows` | Returns tenant-visible account/contact rows using the requested table filter where present. | Canonical accounts and readable contacts. | Supported |
| `ResolveNames` / `ResolveNamesW` | Resolves ANR values against canonical directory rows with deterministic ranking: exact SMTP, display name, and legacy DN matches before prefix/contains matches. | Canonical accounts and readable contacts; hidden self-resolution only for the authenticated principal. | Supported |
| `GetProps` | Returns properties for a requested tenant-visible row, or the authenticated principal for bootstrap requests without a row selector. | Canonical account/contact row projection. | Supported |
| `GetMatches` | Returns matching tenant-visible minimal IDs and row data. | Canonical accounts and readable contacts. | Supported |
| `DNToMId` | Maps tenant-visible legacy DNs and SMTP values to minimal IDs. | Canonical account/contact row projection. | Supported |
| `GetPropList` / `QueryColumns` | Returns the bounded bootstrap property set used by Outlook profile/address-book probes. | No storage mutation. | Supported |
| `SeekEntries` | Returns matching row data over the tenant-visible projection. | Canonical accounts and readable contacts. | Supported |
| `GetSpecialTable` | Returns the bounded Global Address List hierarchy row. | Static compatibility projection. | Supported |
| `GetTemplateInfo` | Returns bounded principal properties for compatibility probes. | Authenticated principal projection. | Bounded |
| `GetAddressBookUrl` | Returns `/mapi/nspi/` public URL. | Request headers only. | Supported |
| `GetMailboxUrl` | Returns `/mapi/emsmdb/` public URL. | Request headers only. | Supported |
| `ModLinkAtt` / `ModProps` | Returns parseable disabled responses. Address-book mutation must use canonical account/contact APIs. | No mutation. | Deferred |

## Deferred

- Distribution list expansion and membership behavior.
- Full address-book template semantics.
- Exchange parity for every ambiguous-name ranking edge case.

## Validation

Required local validation:

```powershell
cargo test -p lpe-exchange nspi
```

The release/publication gate remains the MAPI over HTTP Outlook readiness path
documented in the MAPI implementation and Outlook cached-mode evidence docs.
