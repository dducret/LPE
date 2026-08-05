---
type: Rust Method
title: fetch_ews_delegates
resource: crates/lpe-exchange/src/tests/mod.rs#L5846-L5859
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate
---

# Signature

`fn fetch_ews_delegates<'a>( &'a self, owner_account_id: Uuid, ) -> StoreFuture<'a, Vec<EwsDelegate>>`

# Called by

- [get_delegate](../../../../../../../functions/crates/lpe-exchange/src/service/ews/delegation/ExchangeService/get_delegate.md)