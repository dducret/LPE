---
type: Rust Function
title: blob_lookup_index
resource: crates/lpe-jmap/src/blob.rs#L408-L422
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/upload/blob_id_for_message
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup
---

# Signature

`fn blob_lookup_index(emails: &[JmapEmail]) -> HashMap<String, BlobLookupEntry>`

# Calls

- [blob_id_for_message](../../../../../functions/crates/lpe-jmap/src/upload/blob_id_for_message.md)
- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [handle_blob_lookup](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_lookup.md)