---
type: Rust Function
title: requested_im_group_name
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L468-L474
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group
---

# Signature

`pub(in crate::service) fn requested_im_group_name(request: &str) -> Option<String>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [add_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_group.md)
- [set_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group.md)