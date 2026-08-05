---
type: Rust Method
title: copy_item
resource: crates/lpe-exchange/src/service/ews/items.rs#L863-L991
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_clone_input
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml
  - functions/crates/lpe-exchange/src/service/ews/responses/copy_item_success_response
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn copy_item( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [validate_mutating_item_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [requested_public_folder_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [public_folder_item_clone_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_clone_input.md)
- [public_folder_item_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml.md)
- [copy_item_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/copy_item_success_response.md)
- [message_item_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)