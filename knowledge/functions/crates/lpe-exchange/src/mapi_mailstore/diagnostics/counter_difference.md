---
type: Rust Function
title: counter_difference
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics.rs#L1268-L1276
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/difference
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation
---

# Signature

`fn counter_difference(left: &[u64], right: &[u64]) -> Vec<u64>`

# Calls

- [difference](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/difference.md)

# Called by

- [hierarchy_microsoft_payload_comparison](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_microsoft_payload_comparison.md)
- [hierarchy_semantic_validation](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_semantic_validation.md)