---
type: Rust Method
title: get_sharing_metadata
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L8-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/sharing/requested_sharing_kind
  - functions/crates/lpe-exchange/src/service/ews/sharing/get_sharing_metadata_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_sharing_metadata( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_sharing_kind](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/requested_sharing_kind.md)
- [get_sharing_metadata_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/get_sharing_metadata_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)