---
type: Rust Method
title: upsert_mapi_associated_config
resource: crates/lpe-exchange/src/tests/mod.rs#L10349-L10399
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_storage_is_account_scoped
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_preserves_canonical_message_identity
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_create
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update
---

# Signature

`fn upsert_mapi_associated_config<'a>( &'a self, input: crate::store::UpsertMapiAssociatedConfigInput, ) -> StoreFuture<'a, crate::store::MapiAssociatedConfigRecord>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_associated_config_delete_tombstones_identity_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_associated_config_delete_tombstones_identity_in_postgresql.md)
- [mapi_associated_config_storage_is_account_scoped](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_storage_is_account_scoped.md)
- [mapi_associated_config_upsert_preserves_canonical_message_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_preserves_canonical_message_identity.md)
- [mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)
- [commit_mapi_associated_config_create](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_create.md)
- [commit_mapi_associated_config_import](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_import.md)
- [commit_mapi_associated_config_update](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update.md)