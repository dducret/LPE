---
type: Rust Function
title: requested_im_contact_member
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L530-L561
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/ucs/parse_prefixed_uuid
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group
---

# Signature

`pub(in crate::service) fn requested_im_contact_member( request: &str, principal: &AccountPrincipal, ) -> Option<EwsImMemberInput>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [parse_prefixed_uuid](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/parse_prefixed_uuid.md)
- [requested_im_member_value](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value.md)

# Called by

- [add_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group.md)