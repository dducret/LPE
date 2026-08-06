---
type: Rust Method
title: get_folder
resource: crates/lpe-exchange/src/service/ews/folders.rs#L267-L431
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/responses/get_folder_error_response
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/get_folder_success_response
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kinds
  - functions/crates/lpe-exchange/src/service/ews/request_ids/request_contains_folder_reference
  - functions/crates/lpe-exchange/src/service/ews/folders/root_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/root_child_folder_count
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_folder( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [get_folder_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/get_folder_error_response.md)
- [mailbox_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml.md)
- [get_folder_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/get_folder_success_response.md)
- [requested_public_folder_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [public_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml.md)
- [requested_folder_kinds](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kinds.md)
- [request_contains_folder_reference](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/request_contains_folder_reference.md)
- [root_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/root_folder_xml.md)
- [root_child_folder_count](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/root_child_folder_count.md)
- [collection_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)