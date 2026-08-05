---
type: Rust Function
title: get_non_indexable_item_statistics_response
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L330-L365
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_non_indexable_item_statistics
---

# Signature

`pub(in crate::service) fn get_non_indexable_item_statistics_response( reports: &[EwsNonIndexableReport], ) -> String`

# Calls

- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [get_non_indexable_item_statistics](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_non_indexable_item_statistics.md)