---
type: Rust Method
title: add_new_tel_uri_contact_to_group
resource: crates/lpe-exchange/src/service/ews/ucs.rs#L176-L213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_ews_im_group_member
  - functions/crates/lpe-exchange/src/service/ews/ucs/im_member_operation_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn add_new_tel_uri_contact_to_group( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_im_group_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/requested_im_group_id.md)
- [element_text](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [add_ews_im_group_member](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/add_ews_im_group_member.md)
- [im_member_operation_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/im_member_operation_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)