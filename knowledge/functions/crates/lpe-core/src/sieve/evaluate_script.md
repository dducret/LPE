---
type: Rust Function
title: evaluate_script
resource: crates/lpe-core/src/sieve.rs#L125-L130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/execute_block
  called_by:
  - functions/crates/lpe-core/src/sieve/evaluates_fileinto_and_stop
  - functions/crates/lpe-core/src/sieve/evaluates_redirect_and_vacation_without_cancelling_keep
  - functions/crates/lpe-core/src/sieve/discard_cancels_keep
  - functions/crates/lpe-storage/src/inbound/Storage/evaluate_inbound_sieve
---

# Signature

`pub fn evaluate_script(script: &Script, context: &MessageContext) -> Result<ExecutionOutcome>`

# Calls

- [execute_block](../../../../../functions/crates/lpe-core/src/sieve/execute_block.md)

# Called by

- [evaluates_fileinto_and_stop](../../../../../functions/crates/lpe-core/src/sieve/evaluates_fileinto_and_stop.md)
- [evaluates_redirect_and_vacation_without_cancelling_keep](../../../../../functions/crates/lpe-core/src/sieve/evaluates_redirect_and_vacation_without_cancelling_keep.md)
- [discard_cancels_keep](../../../../../functions/crates/lpe-core/src/sieve/discard_cancels_keep.md)
- [evaluate_inbound_sieve](../../../../../functions/crates/lpe-storage/src/inbound/Storage/evaluate_inbound_sieve.md)