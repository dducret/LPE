---
type: Rust Function
title: parse_file_attachment_upload
resource: crates/lpe-exchange/src/service/ews/attachments.rs#L249-L275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment
---

# Signature

`pub(in crate::service) fn parse_file_attachment_upload( value: &str, ) -> Result<AttachmentUploadInput>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [create_attachment](../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment.md)