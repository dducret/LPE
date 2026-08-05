---
type: Rust Function
title: execute_action
resource: crates/lpe-core/src/sieve.rs#L167-L211
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-core/src/sieve/execute_block
---

# Signature

`fn execute_action(action: &Action, outcome: &mut ExecutionOutcome, stopped: &mut bool)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [execute_block](../../../../../functions/crates/lpe-core/src/sieve/execute_block.md)