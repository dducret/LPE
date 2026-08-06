---
type: Rust Function
title: find_item_page_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L243-L265
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
  - functions/crates/lpe-exchange/src/service/ews/responses/find_item_response
---

# Signature

`pub(in crate::service) fn find_item_page_response( items: String, total_items: u64, includes_last: bool, ) -> String`

# Called by

- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
- [find_item_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/find_item_response.md)