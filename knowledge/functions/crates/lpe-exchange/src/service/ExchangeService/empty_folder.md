---
type: Rust Method
title: empty_folder
resource: crates/lpe-exchange/src/service.rs#L613-L641
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids_in
  - functions/crates/lpe-exchange/src/service/ExchangeService/empty_public_folder
  - functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in
  - functions/crates/lpe-exchange/src/service/ExchangeService/empty_mailbox_folder
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`async fn empty_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String>`

# Calls

- [requested_public_folder_ids_in](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids_in.md)
- [empty_public_folder](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_public_folder.md)
- [simple_operation_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response.md)
- [requested_mailbox_folder_ids_in](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_folder_ids_in.md)
- [empty_mailbox_folder](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_mailbox_folder.md)
- [operation_error_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)