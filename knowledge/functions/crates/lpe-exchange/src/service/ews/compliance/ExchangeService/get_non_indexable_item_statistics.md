---
type: Rust Method
title: get_non_indexable_item_statistics
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L101-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_non_indexable_reports
  - functions/crates/lpe-exchange/src/service/ews/compliance/get_non_indexable_item_statistics_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_non_indexable_item_statistics( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_non_indexable_reports](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_non_indexable_reports.md)
- [get_non_indexable_item_statistics_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/get_non_indexable_item_statistics_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)