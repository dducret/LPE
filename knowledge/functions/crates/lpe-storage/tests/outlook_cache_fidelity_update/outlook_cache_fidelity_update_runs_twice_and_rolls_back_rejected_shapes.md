---
type: Rust Function
title: outlook_cache_fidelity_update_runs_twice_and_rolls_back_rejected_shapes
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L13-L38
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`async fn outlook_cache_fidelity_update_runs_twice_and_rolls_back_rejected_shapes() -> Result<()>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)