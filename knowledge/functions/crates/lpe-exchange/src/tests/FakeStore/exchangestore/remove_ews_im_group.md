---
type: Rust Method
title: remove_ews_im_group
resource: crates/lpe-exchange/src/tests/mod.rs#L7574-L7591
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_group
---

# Signature

`fn remove_ews_im_group<'a>( &'a self, _principal: &'a AccountPrincipal, group_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, bool>`

# Called by

- [remove_im_group](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ucs/ExchangeService/remove_im_group.md)