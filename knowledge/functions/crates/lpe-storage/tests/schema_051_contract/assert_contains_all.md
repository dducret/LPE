---
type: Rust Function
title: assert_contains_all
resource: crates/lpe-storage/tests/schema_051_contract.rs#L226-L230
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/schema_051_contract/update_script_rejects_noncanonical_schema_before_service_stop_or_mutation
  - functions/crates/lpe-storage/tests/schema_051_contract/active_source_key_index_guard_checks_semantics_in_validation_script
  - functions/crates/lpe-storage/tests/schema_051_contract/installation_scripts_validate_the_mapi_store_identity_singleton
  - functions/crates/lpe-storage/tests/schema_051_contract/source_preflight_is_read_only_and_checks_known_050_shape_deltas
  - functions/crates/lpe-storage/tests/schema_051_contract/schema_transition_is_transactional_idempotent_and_version_bounded
---

# Signature

`fn assert_contains_all(label: &str, source: &str, needles: &[&str])`

# Called by

- [update_script_rejects_noncanonical_schema_before_service_stop_or_mutation](../../../../../functions/crates/lpe-storage/tests/schema_051_contract/update_script_rejects_noncanonical_schema_before_service_stop_or_mutation.md)
- [active_source_key_index_guard_checks_semantics_in_validation_script](../../../../../functions/crates/lpe-storage/tests/schema_051_contract/active_source_key_index_guard_checks_semantics_in_validation_script.md)
- [installation_scripts_validate_the_mapi_store_identity_singleton](../../../../../functions/crates/lpe-storage/tests/schema_051_contract/installation_scripts_validate_the_mapi_store_identity_singleton.md)
- [source_preflight_is_read_only_and_checks_known_050_shape_deltas](../../../../../functions/crates/lpe-storage/tests/schema_051_contract/source_preflight_is_read_only_and_checks_known_050_shape_deltas.md)
- [schema_transition_is_transactional_idempotent_and_version_bounded](../../../../../functions/crates/lpe-storage/tests/schema_051_contract/schema_transition_is_transactional_idempotent_and_version_bounded.md)