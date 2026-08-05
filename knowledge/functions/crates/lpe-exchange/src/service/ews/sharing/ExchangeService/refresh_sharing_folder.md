---
type: Rust Method
title: refresh_sharing_folder
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L68-L102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id
  - functions/crates/lpe-exchange/src/service/ews/sharing/refresh_sharing_folder_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn refresh_sharing_folder( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_collection_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id.md)
- [refresh_sharing_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/refresh_sharing_folder_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)