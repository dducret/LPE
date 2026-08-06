---
type: Rust Function
title: find_item_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L238-L241
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/count_tag_occurrences
  - functions/crates/lpe-exchange/src/service/ews/responses/find_item_page_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
---

# Signature

`pub(in crate::service) fn find_item_response(items: String) -> String`

# Calls

- [count_tag_occurrences](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/count_tag_occurrences.md)
- [find_item_page_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/find_item_page_response.md)

# Called by

- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)