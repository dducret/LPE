---
type: Rust Function
title: requested_im_member_kind
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L476-L496
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group
---

# Signature

`pub(in crate::service) fn requested_im_member_kind(request: &str) -> Option<&'static str>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [remove_contact_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list.md)
- [remove_im_contact_from_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group.md)