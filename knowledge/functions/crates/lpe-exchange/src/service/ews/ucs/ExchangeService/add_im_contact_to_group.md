---
type: Rust Method
title: add_im_contact_to_group
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L95-L119
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_contact_member
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_ews_im_group_member
  - functions/crates/lpe-exchange/src/service/ews/ucs/im_member_operation_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn add_im_contact_to_group( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_im_group_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id.md)
- [requested_im_contact_member](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_contact_member.md)
- [add_ews_im_group_member](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_ews_im_group_member.md)
- [im_member_operation_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/im_member_operation_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)