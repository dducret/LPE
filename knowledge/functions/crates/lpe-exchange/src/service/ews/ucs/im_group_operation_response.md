---
type: Rust Function
title: im_group_operation_response
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L389-L407
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group
---

# Signature

`pub(in crate::service) fn im_group_operation_response( operation: &str, group: &EwsImGroup, ) -> String`

# Called by

- [add_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_group.md)
- [set_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group.md)