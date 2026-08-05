---
type: Rust Method
title: get_im_items
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L16-L23
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/get_im_items_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_im_items( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [fetch_ews_im_list](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_im_list.md)
- [get_im_items_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/get_im_items_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)