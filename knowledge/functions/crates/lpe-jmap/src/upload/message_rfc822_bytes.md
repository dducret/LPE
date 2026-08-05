---
type: Rust Function
title: message_rfc822_bytes
resource: crates/lpe-jmap/src/upload.rs#L103-L168
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/upload/push_header
  - functions/crates/lpe-jmap/src/upload/header_address
  - functions/crates/lpe-jmap/src/upload/push_address_list
  - functions/crates/lpe-domain/src/mail_format/rfc5322_utc_date
  - functions/crates/lpe-jmap/src/upload/push_mime_part
  - functions/crates/lpe-domain/src/mail_format/normalize_mime_body
  called_by:
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob_with_bcc
---

# Signature

`pub(crate) fn message_rfc822_bytes(email: &JmapEmail, include_bcc: bool) -> Vec<u8>`

# Calls

- [push_header](../../../../../functions/crates/lpe-jmap/src/upload/push_header.md)
- [header_address](../../../../../functions/crates/lpe-jmap/src/upload/header_address.md)
- [push_address_list](../../../../../functions/crates/lpe-jmap/src/upload/push_address_list.md)
- [rfc5322_utc_date](../../../../../functions/crates/lpe-domain/src/mail_format/rfc5322_utc_date.md)
- [push_mime_part](../../../../../functions/crates/lpe-jmap/src/upload/push_mime_part.md)
- [normalize_mime_body](../../../../../functions/crates/lpe-domain/src/mail_format/normalize_mime_body.md)

# Called by

- [resolve_download_blob_with_bcc](../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob_with_bcc.md)