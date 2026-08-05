---
type: Rust Function
title: execute_block
resource: crates/lpe-core/src/sieve.rs#L132-L165
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/execute_action
  - functions/crates/lpe-core/src/sieve/evaluate_test
  called_by:
  - functions/crates/lpe-core/src/sieve/evaluate_script
---

# Signature

`fn execute_block( statements: &[Statement], context: &MessageContext, outcome: &mut ExecutionOutcome, stopped: &mut bool, ) -> Result<()>`

# Calls

- [execute_action](../../../../../functions/crates/lpe-core/src/sieve/execute_action.md)
- [evaluate_test](../../../../../functions/crates/lpe-core/src/sieve/evaluate_test.md)

# Called by

- [evaluate_script](../../../../../functions/crates/lpe-core/src/sieve/evaluate_script.md)