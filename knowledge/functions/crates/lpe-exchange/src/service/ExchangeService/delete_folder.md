---
type: Rust Method
title: delete_folder
resource: crates/lpe-exchange/src/service.rs#L1015-L1064
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/folders/delete_folder_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`async fn delete_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String>`

# Calls

- [requested_public_folder_ids](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [delete_folder_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/delete_folder_success_response.md)
- [operation_error_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)