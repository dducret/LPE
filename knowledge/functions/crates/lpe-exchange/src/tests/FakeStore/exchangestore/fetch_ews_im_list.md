---
type: Rust Method
title: fetch_ews_im_list
resource: crates/lpe-exchange/src/tests/mod.rs#L7608-L7615
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/get_im_item_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/get_im_items
---

# Signature

`fn fetch_ews_im_list<'a>( &'a self, _principal: &'a AccountPrincipal, ) -> StoreFuture<'a, EwsImList>`

# Called by

- [get_im_item_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/get_im_item_list.md)
- [get_im_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/get_im_items.md)