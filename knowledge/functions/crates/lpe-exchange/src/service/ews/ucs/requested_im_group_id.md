---
type: Rust Function
title: requested_im_group_id
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L448-L466
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/ucs/parse_prefixed_uuid
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_tel_uri_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list
---

# Signature

`pub(in crate::service) fn requested_im_group_id(request: &str) -> Option<Uuid>`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [parse_prefixed_uuid](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/parse_prefixed_uuid.md)

# Called by

- [set_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group.md)
- [remove_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_group.md)
- [add_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group.md)
- [add_new_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group.md)
- [add_new_tel_uri_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_tel_uri_contact_to_group.md)
- [remove_im_contact_from_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group.md)
- [add_distribution_group_to_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list.md)
- [remove_distribution_group_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list.md)