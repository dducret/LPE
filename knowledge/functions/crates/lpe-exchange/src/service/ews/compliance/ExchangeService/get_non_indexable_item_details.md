---
type: Rust Method
title: get_non_indexable_item_details
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L90-L99
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_non_indexable_reports
  - functions/crates/lpe-exchange/src/service/ews/compliance/get_non_indexable_item_details_response
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_non_indexable_item_details( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [fetch_ews_non_indexable_reports](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_ews_non_indexable_reports.md)
- [get_non_indexable_item_details_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/get_non_indexable_item_details_response.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)