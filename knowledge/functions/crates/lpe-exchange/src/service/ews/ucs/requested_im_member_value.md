---
type: Rust Function
title: requested_im_member_value
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L498-L528
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_smtp_address
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_contact_member
---

# Signature

`pub(in crate::service) fn requested_im_member_value(request: &str) -> Option<String>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [requested_smtp_address](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_smtp_address.md)

# Called by

- [remove_contact_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list.md)
- [remove_im_contact_from_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group.md)
- [requested_im_contact_member](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_contact_member.md)