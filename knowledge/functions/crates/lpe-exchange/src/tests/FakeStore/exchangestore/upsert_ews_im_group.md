---
type: Rust Method
title: upsert_ews_im_group
resource: crates/lpe-exchange/src/tests/mod.rs#L7622-L7646
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_group
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group
---

# Signature

`fn upsert_ews_im_group<'a>( &'a self, _principal: &'a AccountPrincipal, group_id: Option<Uuid>, display_name: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsImGroup>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [add_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/add_im_group.md)
- [set_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/set_im_group.md)