---
type: Rust Module
title: jmap_blobs
resource: crates/lpe-storage/src/jmap_blobs.rs#L1-L359
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/serde-serialize
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-accountquotarow-jmapuploadblobrow-storage
  - external/super-strip-protected-bcc-headers
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [JmapQuota](../../../../classes/crates/lpe-storage/src/jmap_blobs/JmapQuota.md)
- [JmapUploadBlob](../../../../classes/crates/lpe-storage/src/jmap_blobs/JmapUploadBlob.md)
- [fetch_jmap_quota](../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_jmap_quota.md)
- [fetch_mailbox_logical_quota_used_octets](../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_mailbox_logical_quota_used_octets.md)
- [fetch_domain_logical_quota_used_octets](../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_domain_logical_quota_used_octets.md)
- [fetch_jmap_message_blob](../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_jmap_message_blob.md)
- [fetch_jmap_message_blob_with_protected_headers](../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_jmap_message_blob_with_protected_headers.md)
- [save_jmap_upload_blob](../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/save_jmap_upload_blob.md)
- [fetch_jmap_upload_blob](../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_jmap_upload_blob.md)
- [strip_protected_bcc_headers](../../../../functions/crates/lpe-storage/src/jmap_blobs/strip_protected_bcc_headers.md)
- [header_name_is_bcc](../../../../functions/crates/lpe-storage/src/jmap_blobs/header_name_is_bcc.md)
- [protected_bcc_headers_are_stripped_from_default_message_blobs](../../../../functions/crates/lpe-storage/src/jmap_blobs/protected_bcc_headers_are_stripped_from_default_message_blobs.md)

# Imports

- `anyhow::{anyhow, Result}`
- `serde::Serialize`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{AccountQuotaRow, JmapUploadBlobRow, Storage}`
- `super::strip_protected_bcc_headers`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)