---
type: Rust Function
title: get_im_items_response
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L363-L387
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-exchange/src/service/ews/ucs/im_member_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/get_im_items
---

# Signature

`pub(in crate::service) fn get_im_items_response(request: &str, list: &EwsImList) -> String`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [im_member_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/im_member_id.md)

# Called by

- [get_im_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/get_im_items.md)