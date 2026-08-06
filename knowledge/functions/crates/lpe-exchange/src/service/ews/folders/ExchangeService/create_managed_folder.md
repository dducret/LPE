---
type: Rust Method
title: create_managed_folder
resource: crates/lpe-exchange/src/service/ews/folders.rs#L201-L250
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/folders_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn create_managed_folder( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_contents](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [mailbox_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml.md)
- [folders_operation_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/folders_operation_success_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)