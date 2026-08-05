---
type: Rust Function
title: simple_ews_operation_result
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L429-L435
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list
---

# Signature

`pub(in crate::service) fn simple_ews_operation_result(operation: &str, ok: bool) -> String`

# Calls

- [simple_operation_success_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/simple_operation_success_response.md)
- [operation_error_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)

# Called by

- [remove_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_group.md)
- [remove_contact_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list.md)
- [remove_im_contact_from_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group.md)
- [remove_distribution_group_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list.md)