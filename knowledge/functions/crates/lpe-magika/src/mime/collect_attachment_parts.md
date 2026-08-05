---
type: Rust Function
title: collect_attachment_parts
resource: crates/lpe-magika/src/mime.rs#L63-L109
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes
  - functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-magika/src/mime/split_multipart_parts
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
---

# Signature

`fn collect_attachment_parts(bytes: &[u8], attachments: &mut Vec<MimeAttachmentPart>) -> Result<()>`

# Calls

- [split_headers_and_body_bytes](../../../../../functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes.md)
- [parse_rfc822_headers_bytes](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [split_multipart_parts](../../../../../functions/crates/lpe-magika/src/mime/split_multipart_parts.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [collect_mime_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)