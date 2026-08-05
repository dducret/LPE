---
type: Rust Method
title: get_sharing_folder
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L41-L66
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/resolve_same_tenant_account
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accessible_shared_collection
  - functions/crates/lpe-exchange/src/service/ews/sharing/get_sharing_folder_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_sharing_folder( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [parse_sharing_request](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request.md)
- [resolve_same_tenant_account](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/resolve_same_tenant_account.md)
- [accessible_shared_collection](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accessible_shared_collection.md)
- [get_sharing_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/get_sharing_folder_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)