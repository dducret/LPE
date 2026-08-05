---
type: Rust Method
title: add_ews_im_group_member
resource: crates/lpe-exchange/src/tests/mod.rs#L7536-L7584
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_tel_uri_contact_to_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list
---

# Signature

`fn add_ews_im_group_member<'a>( &'a self, _principal: &'a AccountPrincipal, group_id: Uuid, member: EwsImMemberInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsImGroupMember>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [add_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_contact_to_group.md)
- [add_new_im_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_im_contact_to_group.md)
- [add_new_tel_uri_contact_to_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_new_tel_uri_contact_to_group.md)
- [add_distribution_group_to_im_list](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_distribution_group_to_im_list.md)