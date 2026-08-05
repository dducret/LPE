---
type: Rust Function
title: blob_id_for_message
resource: crates/lpe-jmap/src/upload.rs#L47-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/upload/JmapBlobId/for_message
  - functions/crates/lpe-jmap/src/upload/JmapBlobId/into_response_id
  called_by:
  - functions/crates/lpe-jmap/src/blob/blob_lookup_index
  - functions/crates/lpe-jmap/src/mail/values/email_to_value
---

# Signature

`pub(crate) fn blob_id_for_message(email: &JmapEmail) -> String`

# Calls

- [for_message](../../../../../functions/crates/lpe-jmap/src/upload/JmapBlobId/for_message.md)
- [into_response_id](../../../../../functions/crates/lpe-jmap/src/upload/JmapBlobId/into_response_id.md)

# Called by

- [blob_lookup_index](../../../../../functions/crates/lpe-jmap/src/blob/blob_lookup_index.md)
- [email_to_value](../../../../../functions/crates/lpe-jmap/src/mail/values/email_to_value.md)