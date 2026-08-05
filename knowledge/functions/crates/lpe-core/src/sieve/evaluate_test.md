---
type: Rust Function
title: evaluate_test
resource: crates/lpe-core/src/sieve.rs#L213-L258
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/matches_any
  - functions/crates/lpe-core/src/sieve/extract_addresses
  called_by:
  - functions/crates/lpe-core/src/sieve/execute_block
---

# Signature

`fn evaluate_test(test: &Test, context: &MessageContext) -> bool`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [matches_any](../../../../../functions/crates/lpe-core/src/sieve/matches_any.md)
- [extract_addresses](../../../../../functions/crates/lpe-core/src/sieve/extract_addresses.md)

# Called by

- [execute_block](../../../../../functions/crates/lpe-core/src/sieve/execute_block.md)