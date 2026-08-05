---
type: Rust Function
title: strip_protected_bcc_headers
resource: crates/lpe-storage/src/jmap_blobs.rs#L294-L333
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-storage/src/jmap_blobs/header_name_is_bcc
  called_by:
  - functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_jmap_message_blob
  - functions/crates/lpe-storage/src/jmap_blobs/protected_bcc_headers_are_stripped_from_default_message_blobs
---

# Signature

`fn strip_protected_bcc_headers(raw: &[u8]) -> Vec<u8>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [header_name_is_bcc](../../../../../functions/crates/lpe-storage/src/jmap_blobs/header_name_is_bcc.md)

# Called by

- [fetch_jmap_message_blob](../../../../../functions/crates/lpe-storage/src/jmap_blobs/Storage/fetch_jmap_message_blob.md)
- [protected_bcc_headers_are_stripped_from_default_message_blobs](../../../../../functions/crates/lpe-storage/src/jmap_blobs/protected_bcc_headers_are_stripped_from_default_message_blobs.md)