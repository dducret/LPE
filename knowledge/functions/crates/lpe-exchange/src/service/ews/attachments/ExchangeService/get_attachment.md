---
type: Rust Method
title: get_attachment
resource: crates/lpe-exchange/src/service/ews/attachments.rs#L8-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_attachment_ids
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content
  - functions/crates/lpe-exchange/src/service/ews/mime/file_attachment_content_xml
  - functions/crates/lpe-exchange/src/service/ews/attachments/get_attachment_success_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_attachment( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_attachment_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_attachment_ids.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [fetch_attachment_content](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content.md)
- [file_attachment_content_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mime/file_attachment_content_xml.md)
- [get_attachment_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/get_attachment_success_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)