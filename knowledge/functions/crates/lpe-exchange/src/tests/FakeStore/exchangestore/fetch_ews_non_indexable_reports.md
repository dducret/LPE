---
type: Rust Method
title: fetch_ews_non_indexable_reports
resource: crates/lpe-exchange/src/tests/mod.rs#L5462-L5486
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_non_indexable_item_details
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_non_indexable_item_statistics
---

# Signature

`fn fetch_ews_non_indexable_reports<'a>( &'a self, principal: &'a AccountPrincipal, ) -> StoreFuture<'a, Vec<EwsNonIndexableReport>>`

# Called by

- [get_non_indexable_item_details](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_non_indexable_item_details.md)
- [get_non_indexable_item_statistics](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_non_indexable_item_statistics.md)