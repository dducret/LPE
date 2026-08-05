---
type: Rust Method
title: delete_attachment
resource: crates/lpe-exchange/src/service/ews/attachments.rs#L154-L193
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_attachment_ids
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/mail/root_item_id_xml
  - functions/crates/lpe-exchange/src/service/ews/attachments/delete_attachment_success_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn delete_attachment( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_attachment_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_attachment_ids.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [root_item_id_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/root_item_id_xml.md)
- [delete_attachment_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/delete_attachment_success_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)