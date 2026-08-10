---
type: Rust Method
title: create_folder
resource: crates/lpe-exchange/src/service.rs#L388-L440
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/folders/create_public_folder_success_response
  - functions/crates/lpe-exchange/src/service/ews/folders/create_folder_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`async fn create_folder(&self, principal: &AccountPrincipal, request: &str) -> Result<String>`

# Calls

- [element_text](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [requested_public_folder_ids](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [create_public_folder_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/create_public_folder_success_response.md)
- [create_folder_success_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/create_folder_success_response.md)
- [operation_error_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [handle](../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)