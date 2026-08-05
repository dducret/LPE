---
type: Rust Method
title: create_attachment
resource: crates/lpe-exchange/src/service/ews/attachments.rs#L41-L152
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/attachments/parse_file_attachment_upload
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/mail/root_item_id_xml
  - functions/crates/lpe-exchange/src/service/ews/mime/file_attachment_reference_xml
  - functions/crates/lpe-exchange/src/service/ews/attachments/create_attachment_success_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn create_attachment( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [validate_mutating_item_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)
- [element_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_contents](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [parse_file_attachment_upload](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/parse_file_attachment_upload.md)
- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [root_item_id_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/root_item_id_xml.md)
- [file_attachment_reference_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/file_attachment_reference_xml.md)
- [create_attachment_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/create_attachment_success_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)