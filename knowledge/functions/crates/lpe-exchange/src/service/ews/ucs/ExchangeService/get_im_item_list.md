---
type: Rust Method
title: get_im_item_list
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L8-L14
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/get_im_item_list_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_im_item_list( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_im_list](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_im_list.md)
- [get_im_item_list_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/get_im_item_list_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)