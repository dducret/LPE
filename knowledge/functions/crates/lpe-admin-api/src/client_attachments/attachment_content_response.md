---
type: Rust Function
title: attachment_content_response
resource: crates/lpe-admin-api/src/client_attachments.rs#L230-L250
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_attachments/normalized_attachment_file_name
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-admin-api/src/client_attachments/download_message_attachment
  - functions/crates/lpe-admin-api/src/client_attachments/attachment_download_uses_safe_inline_headers
---

# Signature

`fn attachment_content_response(file_name: &str, media_type: &str, blob_bytes: Vec<u8>) -> Response`

# Calls

- [normalized_attachment_file_name](../../../../../functions/crates/lpe-admin-api/src/client_attachments/normalized_attachment_file_name.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [download_message_attachment](../../../../../functions/crates/lpe-admin-api/src/client_attachments/download_message_attachment.md)
- [attachment_download_uses_safe_inline_headers](../../../../../functions/crates/lpe-admin-api/src/client_attachments/attachment_download_uses_safe_inline_headers.md)