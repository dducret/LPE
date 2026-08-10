---
type: Rust Method
title: upsert_ews_delegate
resource: crates/lpe-exchange/src/tests/mod.rs#L5935-L5974
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates
---

# Signature

`fn upsert_ews_delegate<'a>( &'a self, input: UpsertEwsDelegateInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, EwsDelegate>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mutate_ews_delegates](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/mutate_ews_delegates.md)