---
type: Rust Method
title: set_im_group
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L48-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_name
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_ews_im_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/im_group_operation_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn set_im_group( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_im_group_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id.md)
- [requested_im_group_name](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_name.md)
- [upsert_ews_im_group](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_ews_im_group.md)
- [im_group_operation_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/im_group_operation_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)