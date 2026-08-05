---
type: Rust Function
title: assert_before
resource: crates/lpe-storage/tests/schema_051_contract.rs#L232-L240
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/schema_051_contract/update_script_rejects_noncanonical_schema_before_service_stop_or_mutation
  - functions/crates/lpe-storage/tests/schema_051_contract/schema_transition_is_transactional_idempotent_and_version_bounded
---

# Signature

`fn assert_before(source: &str, earlier: &str, later: &str, message: &str)`

# Called by

- [update_script_rejects_noncanonical_schema_before_service_stop_or_mutation](../../../../../functions/crates/lpe-storage/tests/schema_051_contract/update_script_rejects_noncanonical_schema_before_service_stop_or_mutation.md)
- [schema_transition_is_transactional_idempotent_and_version_bounded](../../../../../functions/crates/lpe-storage/tests/schema_051_contract/schema_transition_is_transactional_idempotent_and_version_bounded.md)