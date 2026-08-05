---
type: Rust Method
title: remove_ews_im_group_member
resource: crates/lpe-exchange/src/tests/mod.rs#L7586-L7619
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list
---

# Signature

`fn remove_ews_im_group_member<'a>( &'a self, _principal: &'a AccountPrincipal, group_id: Option<Uuid>, member_kind: &'a str, member_value: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, bool>`

# Called by

- [remove_contact_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_contact_from_im_list.md)
- [remove_im_contact_from_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_contact_from_group.md)
- [remove_distribution_group_from_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_distribution_group_from_im_list.md)