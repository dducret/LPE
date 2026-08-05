---
type: Rust Function
title: parse_prefixed_uuid
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L614-L616
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_contact_member
---

# Signature

`fn parse_prefixed_uuid(value: &str, prefix: &str) -> Option<Uuid>`

# Called by

- [requested_im_group_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id.md)
- [requested_im_contact_member](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_contact_member.md)