---
type: Rust Function
title: requested_smtp_address
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L437-L446
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value
---

# Signature

`pub(in crate::service) fn requested_smtp_address(request: &str) -> Option<String>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)

# Called by

- [add_new_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group.md)
- [add_distribution_group_to_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list.md)
- [remove_distribution_group_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list.md)
- [requested_im_member_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value.md)