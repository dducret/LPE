---
type: Rust Method
title: remove_im_contact_from_group
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L242-L269
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/remove_ews_im_group_member
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_kind
  - functions/crates/lpe-exchange/src/service/ews/ucs/simple_ews_operation_result
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn remove_im_contact_from_group( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_im_group_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id.md)
- [requested_im_member_value](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_value.md)
- [remove_ews_im_group_member](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/remove_ews_im_group_member.md)
- [requested_im_member_kind](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_member_kind.md)
- [simple_ews_operation_result](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/simple_ews_operation_result.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)