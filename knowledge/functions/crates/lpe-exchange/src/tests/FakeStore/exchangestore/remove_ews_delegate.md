---
type: Rust Method
title: remove_ews_delegate
resource: crates/lpe-exchange/src/tests/mod.rs#L5970-L5984
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate
---

# Signature

`fn remove_ews_delegate<'a>( &'a self, owner_account_id: Uuid, grantee_account_id: Uuid, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, bool>`

# Called by

- [remove_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/remove_delegate.md)