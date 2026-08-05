---
type: Rust Function
title: im_member_operation_response
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L409-L427
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_tel_uri_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list
---

# Signature

`pub(in crate::service) fn im_member_operation_response( operation: &str, member: &EwsImGroupMember, ) -> String`

# Called by

- [add_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group.md)
- [add_new_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group.md)
- [add_new_tel_uri_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_tel_uri_contact_to_group.md)
- [add_distribution_group_to_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list.md)