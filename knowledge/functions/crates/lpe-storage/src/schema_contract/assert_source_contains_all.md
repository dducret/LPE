---
type: Rust Function
title: assert_source_contains_all
resource: crates/lpe-storage/src/schema_contract.rs#L124-L131
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/schema_contract/public_folder_schema_uses_canonical_tables_permissions_and_replay
  - functions/crates/lpe-storage/src/schema_contract/calendar_event_mutations_advance_canonical_and_mapi_versions
  - functions/crates/lpe-storage/src/schema_contract/mapi_delegate_freebusy_messages_are_computed_from_canonical_state
  - functions/crates/lpe-storage/src/schema_contract/mapi_profile_settings_are_canonical_account_settings
  - functions/crates/lpe-storage/src/schema_contract/outlook_cache_fidelity_update_is_transactional_idempotent_and_version_bounded
  - functions/crates/lpe-storage/src/schema_contract/local_replica_range_update_rejects_preexisting_incomplete_tables
  - functions/crates/lpe-storage/src/schema_contract/check_script_rejects_tagged_schema_without_mapi_identity_version_columns
  - functions/crates/lpe-storage/src/schema_contract/updater_rejects_an_incomplete_current_schema_before_stopping_lpe
  - functions/crates/lpe-storage/src/schema_contract/deployment_scripts_require_local_replica_range_table_shape
  - functions/crates/lpe-storage/src/schema_contract/deployment_and_runtime_guards_require_outlook_cache_fidelity_shape
  - functions/crates/lpe-storage/src/schema_contract/deployment_scripts_reject_tagged_schema_without_special_folder_alias_shape
  - functions/crates/lpe-storage/src/schema_contract/deployment_and_startup_reject_stale_mapi_change_key_constraints
  - functions/crates/lpe-storage/src/schema_contract/installation_scripts_require_the_mapi_store_identity_singleton
  - functions/crates/lpe-storage/src/schema_contract/schema_initializer_resets_atomically_and_validates_durable_mapi_shape
  - functions/crates/lpe-storage/src/schema_contract/fresh_schema_checks_validate_constraint_shape_without_migration_names
  - functions/crates/lpe-storage/src/schema_contract/runtime_schema_check_rejects_missing_required_mapi_shape
  - functions/crates/lpe-storage/src/schema_contract/cross_protocol_adapter_tests_cover_canonical_model_first_paths
---

# Signature

`fn assert_source_contains_all(name: &str, source: &str, needles: &[&str])`

# Called by

- [public_folder_schema_uses_canonical_tables_permissions_and_replay](../../../../../functions/crates/lpe-storage/src/schema_contract/public_folder_schema_uses_canonical_tables_permissions_and_replay.md)
- [calendar_event_mutations_advance_canonical_and_mapi_versions](../../../../../functions/crates/lpe-storage/src/schema_contract/calendar_event_mutations_advance_canonical_and_mapi_versions.md)
- [mapi_delegate_freebusy_messages_are_computed_from_canonical_state](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_delegate_freebusy_messages_are_computed_from_canonical_state.md)
- [mapi_profile_settings_are_canonical_account_settings](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_profile_settings_are_canonical_account_settings.md)
- [outlook_cache_fidelity_update_is_transactional_idempotent_and_version_bounded](../../../../../functions/crates/lpe-storage/src/schema_contract/outlook_cache_fidelity_update_is_transactional_idempotent_and_version_bounded.md)
- [local_replica_range_update_rejects_preexisting_incomplete_tables](../../../../../functions/crates/lpe-storage/src/schema_contract/local_replica_range_update_rejects_preexisting_incomplete_tables.md)
- [check_script_rejects_tagged_schema_without_mapi_identity_version_columns](../../../../../functions/crates/lpe-storage/src/schema_contract/check_script_rejects_tagged_schema_without_mapi_identity_version_columns.md)
- [updater_rejects_an_incomplete_current_schema_before_stopping_lpe](../../../../../functions/crates/lpe-storage/src/schema_contract/updater_rejects_an_incomplete_current_schema_before_stopping_lpe.md)
- [deployment_scripts_require_local_replica_range_table_shape](../../../../../functions/crates/lpe-storage/src/schema_contract/deployment_scripts_require_local_replica_range_table_shape.md)
- [deployment_and_runtime_guards_require_outlook_cache_fidelity_shape](../../../../../functions/crates/lpe-storage/src/schema_contract/deployment_and_runtime_guards_require_outlook_cache_fidelity_shape.md)
- [deployment_scripts_reject_tagged_schema_without_special_folder_alias_shape](../../../../../functions/crates/lpe-storage/src/schema_contract/deployment_scripts_reject_tagged_schema_without_special_folder_alias_shape.md)
- [deployment_and_startup_reject_stale_mapi_change_key_constraints](../../../../../functions/crates/lpe-storage/src/schema_contract/deployment_and_startup_reject_stale_mapi_change_key_constraints.md)
- [installation_scripts_require_the_mapi_store_identity_singleton](../../../../../functions/crates/lpe-storage/src/schema_contract/installation_scripts_require_the_mapi_store_identity_singleton.md)
- [schema_initializer_resets_atomically_and_validates_durable_mapi_shape](../../../../../functions/crates/lpe-storage/src/schema_contract/schema_initializer_resets_atomically_and_validates_durable_mapi_shape.md)
- [fresh_schema_checks_validate_constraint_shape_without_migration_names](../../../../../functions/crates/lpe-storage/src/schema_contract/fresh_schema_checks_validate_constraint_shape_without_migration_names.md)
- [runtime_schema_check_rejects_missing_required_mapi_shape](../../../../../functions/crates/lpe-storage/src/schema_contract/runtime_schema_check_rejects_missing_required_mapi_shape.md)
- [cross_protocol_adapter_tests_cover_canonical_model_first_paths](../../../../../functions/crates/lpe-storage/src/schema_contract/cross_protocol_adapter_tests_cover_canonical_model_first_paths.md)