---
type: Rust Function
title: requested_transfer_item_ids
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L39-L50
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/xml/element_contents
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/upload_items
---

# Signature

`pub(in crate::service) fn requested_transfer_item_ids(request: &str) -> Vec<String>`

# Calls

- [requested_item_ids](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [element_contents](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [upload_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/bulk_transfer/ExchangeService/upload_items.md)