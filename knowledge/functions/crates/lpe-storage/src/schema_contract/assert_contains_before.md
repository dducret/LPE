---
type: Rust Function
title: assert_contains_before
resource: crates/lpe-storage/src/schema_contract.rs#L93-L101
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/schema_contract/mapi_local_replica_ranges_and_deleted_item_list_are_durable
  - functions/crates/lpe-storage/src/schema_contract/outlook_cache_fidelity_update_is_transactional_idempotent_and_version_bounded
  - functions/crates/lpe-storage/src/schema_contract/local_replica_range_update_rejects_preexisting_incomplete_tables
  - functions/crates/lpe-storage/src/schema_contract/updater_rejects_an_incomplete_current_schema_before_stopping_lpe
  - functions/crates/lpe-storage/src/schema_contract/schema_initializer_resets_atomically_and_validates_durable_mapi_shape
  - functions/crates/lpe-storage/src/schema_contract/collaboration_deletes_write_tombstones
---

# Signature

`fn assert_contains_before(haystack: &str, first: &str, second: &str, message: &str)`

# Called by

- [mapi_local_replica_ranges_and_deleted_item_list_are_durable](../../../../../functions/crates/lpe-storage/src/schema_contract/mapi_local_replica_ranges_and_deleted_item_list_are_durable.md)
- [outlook_cache_fidelity_update_is_transactional_idempotent_and_version_bounded](../../../../../functions/crates/lpe-storage/src/schema_contract/outlook_cache_fidelity_update_is_transactional_idempotent_and_version_bounded.md)
- [local_replica_range_update_rejects_preexisting_incomplete_tables](../../../../../functions/crates/lpe-storage/src/schema_contract/local_replica_range_update_rejects_preexisting_incomplete_tables.md)
- [updater_rejects_an_incomplete_current_schema_before_stopping_lpe](../../../../../functions/crates/lpe-storage/src/schema_contract/updater_rejects_an_incomplete_current_schema_before_stopping_lpe.md)
- [schema_initializer_resets_atomically_and_validates_durable_mapi_shape](../../../../../functions/crates/lpe-storage/src/schema_contract/schema_initializer_resets_atomically_and_validates_durable_mapi_shape.md)
- [collaboration_deletes_write_tombstones](../../../../../functions/crates/lpe-storage/src/schema_contract/collaboration_deletes_write_tombstones.md)