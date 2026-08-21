# NSPI Support Matrix

## Current State/Functionality Overview

`NSPI` is an authenticated address-book compatibility surface for Outlook profile
creation and address resolution. It projects canonical `LPE` tenant directory
accounts and readable canonical contacts. It does not own directory state and
does not mutate address-book data.

The Microsoft `MS-NSPI` reference checked for this matrix on 2026-05-29 was the
published version available from Microsoft Learn on that date.

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
- EWS `ResolveNames` and NSPI use that same canonical readable-contact scope;
  a private contact that NSPI cannot resolve is not exposed through EWS either,
  following [MS-OXNSPI] section 3.1.4.1.16 and [MS-OXWSRSLNM] section 3.1.4.1.
- Distribution-list rows come from canonical group aliases; member enumeration is
  read-only and bounded to canonical group-alias target data that resolves to
  tenant-visible address-book rows.
- Hidden authenticated accounts are not browsed, but can resolve themselves for
  Outlook profile bootstrap.
- Address-book rows use Exchange address-book identity semantics: `PidTagAddressType`
  is `EX`, `PidTagEmailAddress` and
  `PidTagAddressBookObjectDistinguishedName` carry the legacy DN, `PidTagSmtpAddress`
  carries the SMTP address, and EntryID/TemplateID/RecordKey/SearchKey are derived
  from the same legacy DN identity.
- `GetMailboxUrl` and `GetAddressBookUrl` return the real public MAPI/HTTP
  endpoint URLs derived from request host/proxy headers.
- The legacy RPC proxy handles authenticated `RfrGetNewDSA` and
  `RfrGetFQDNFromServerDN` compatibility probes only, returning the configured
  mailbox host. This is the bounded referral behavior in [MS-OXABREF] sections
  3.1.4.1 and 3.1.4.2, not an LPE directory service, multi-DSA router, or a
  reason to publish a separate referral endpoint.

## Reference Table/List

| NSPI method | LPE behavior | Storage boundary | Status |
| --- | --- | --- | --- |
| `Bind` | Creates or reconnects an authenticated NSPI session and returns the server GUID. | Session state only. | Supported |
| `Unbind` | Releases the authenticated NSPI session. | Session state only. | Supported |
| `QueryRows` | Returns tenant-visible account/contact/distribution-list rows using the requested table filter where present; filtered rowsets use the same deterministic ANR ranking as `ResolveNames`. | Canonical accounts, readable contacts, and group aliases. | Supported |
| `ResolveNames` / `ResolveNamesW` | Resolves ANR values against canonical directory rows with deterministic ranking: exact SMTP, display name, and legacy DN matches before prefix/contains matches, with account, distribution-list, then contact tie-breaking. The complete ordered request is validated before reading or allocating address-book projection state; the observed all-zero RCA bootstrap probe remains a no-name principal bootstrap. | Canonical accounts, readable contacts, and group aliases; hidden self-resolution only for the authenticated principal. | Supported |
| `GetProps` | Returns properties for a requested tenant-visible row, or the authenticated principal for bootstrap requests without a row selector. MAPI/HTTP property lists use the `AddressBookPropertyValueList` encoding without RPC-only reserved or alignment fields. Distribution-list member properties are returned only from canonical group-alias membership data that resolves to tenant-visible address-book rows. | Canonical account/contact/distribution-list row projection and bounded canonical group-alias member projection. | Supported |
| `GetMatches` | Returns matching tenant-visible minimal IDs and row data ranked by ANR match quality. | Canonical accounts, readable contacts, and group aliases. | Supported |
| `DNToMId` / `DNToEPH` | Maps tenant-visible legacy DNs and SMTP values to minimal IDs for Outlook address-book bootstrap. | Canonical account/contact/distribution-list row projection. | Supported |
| `GetPropList` / `QueryColumns` | Returns the bounded bootstrap property set used by Outlook profile/address-book probes. | No storage mutation. | Supported |
| `SeekEntries` | Returns matching row data over the tenant-visible projection. | Canonical accounts and readable contacts. | Supported |
| `GetSpecialTable` | Returns the bounded static address-book hierarchy rows for Global Address List, All Users, All Groups, and All Contacts. MAPI/HTTP rows are encoded directly as `AddressBookPropertyValueList` values whose tagged properties contain no RPC alignment field and whose binary and string values include the required `HasValue` byte. The documented properties are returned in order: entry ID, container flags, depth, container ID, display name, and master flag. Permanent entry IDs use the `MS-OXOABK` address-list DN forms: `/` for the GAL and `/guid=<32 hex digits>` for other containers. | Static compatibility projection. | Supported |
| `GetTemplateInfo` | Validates the complete MAPI/HTTP flags, display type, optional ASCII template DN, code page, locale, and exact auxiliary buffer before returning bounded principal properties for compatibility probes using the same `AddressBookPropertyValueList` encoding as `GetProps`. The response is not a claim of a general UI-template catalog. | Authenticated principal projection. | Bounded |
| `GetAddressBookUrl` | Returns `/mapi/nspi/` public URL. | Request headers only. | Supported |
| `GetMailboxUrl` | Returns `/mapi/emsmdb/` public URL. | Request headers only. | Supported |
| `ModLinkAtt` / `ModProps` | Returns parseable disabled responses. Address-book mutation must use canonical account/contact APIs. | No mutation. | Deferred |

## Deferred

- Full address-book template semantics.
- Distribution list expansion beyond canonical group-alias membership targets.
- Distribution-list member mutation through `NSPI`.
- Exchange parity for every ambiguous-name ranking edge case.

`ResolveNames` never scans a malformed wire body for incidental SMTP-like text.
It returns the parseable invalid-request diagnostic before address-book identity
allocation, so malformed or foreign input cannot create a projection or expose
a row. This follows [MS-OXNSPI] section 3.1.4.1.16. The retained all-zero
RCA bootstrap request is an interoperability compatibility exception, not a
second request format.

`GetTemplateInfo` applies the same no-projection-on-malformed-input rule to its
complete MAPI/HTTP request body, following [MS-OXCMAPIHTTP] section 2.2.5.9.1
and [MS-OXNSPI] section 3.1.4.1.18. The full template lookup and rendering
contract in [MS-OXOABKT] section 3.2.5.2 remains explicitly unsupported: LPE
does not create an Exchange-only template catalog or permit NSPI mutations.

## Validation

Required local validation:

```powershell
cargo test -p lpe-exchange nspi
```

NSPI remains part of the MAPI over HTTP 0.5.x release-quality path documented
in the MAPI implementation and Outlook cached-mode evidence docs. A regression
in bind, resolve, or URL discovery is a published Outlook compatibility defect.
