---
type: Rust Module
title: upload
resource: crates/lpe-jmap/src/upload.rs#L1-L206
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/lpe-domain-mail-format-format-mailbox-address-normalize-mime-body-rfc5322-utc-date-sanitize-header-value-displaynamepolicy
  - external/lpe-magika-expectedkind
  - external/lpe-storage-jmapemail-jmapemailaddress
  - external/uuid-uuid
  - external/crate-parse-parse-uuid
  member_of:
  - packages/crates/lpe-jmap
---

# Contains

- [JmapBlobId](../../../../classes/crates/lpe-jmap/src/upload/JmapBlobId.md)
- [parse_upload_blob_id](../../../../functions/crates/lpe-jmap/src/upload/parse_upload_blob_id.md)
- [expected_attachment_kind](../../../../functions/crates/lpe-jmap/src/upload/expected_attachment_kind.md)
- [blob_id_for_message](../../../../functions/crates/lpe-jmap/src/upload/blob_id_for_message.md)
- [parse](../../../../functions/crates/lpe-jmap/src/upload/JmapBlobId/parse.md)
- [for_message](../../../../functions/crates/lpe-jmap/src/upload/JmapBlobId/for_message.md)
- [into_response_id](../../../../functions/crates/lpe-jmap/src/upload/JmapBlobId/into_response_id.md)
- [message_rfc822_bytes](../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)
- [push_mime_part](../../../../functions/crates/lpe-jmap/src/upload/push_mime_part.md)
- [push_header](../../../../functions/crates/lpe-jmap/src/upload/push_header.md)
- [push_address_list](../../../../functions/crates/lpe-jmap/src/upload/push_address_list.md)
- [header_address](../../../../functions/crates/lpe-jmap/src/upload/header_address.md)

# Imports

- `anyhow::{bail, Result}`
- `lpe_domain::mail_format::{
    format_mailbox_address, normalize_mime_body, rfc5322_utc_date, sanitize_header_value,
    DisplayNamePolicy,
}`
- `lpe_magika::ExpectedKind`
- `lpe_storage::{JmapEmail, JmapEmailAddress}`
- `uuid::Uuid`
- `crate::parse::parse_uuid`

# Member of

- [lpe-jmap](../../../../packages/crates/lpe-jmap.md)