---
type: Rust Method
title: fetch_mapi_associated_configs
resource: crates/lpe-exchange/src/tests/mod.rs#L10266-L10279
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_inbox_associated_config_bootstrap_inserts_no_defaults
  - functions/crates/lpe-exchange/src/tests/mapi_inbox_associated_config_bootstrap_preserves_existing_persisted_row
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_storage_is_account_scoped
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_preserves_canonical_message_identity
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects
  - functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state
---

# Signature

`fn fetch_mapi_associated_configs<'a>( &'a self, account_id: Uuid, ) -> StoreFuture<'a, Vec<crate::store::MapiAssociatedConfigRecord>>`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_existing_associated_config_save_is_atomic_in_postgresql.md)
- [mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_associated_config_ignores_client_read_only_properties_in_postgresql.md)
- [mapi_inbox_associated_config_bootstrap_inserts_no_defaults](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_inbox_associated_config_bootstrap_inserts_no_defaults.md)
- [mapi_inbox_associated_config_bootstrap_preserves_existing_persisted_row](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_inbox_associated_config_bootstrap_preserves_existing_persisted_row.md)
- [mapi_associated_config_storage_is_account_scoped](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_storage_is_account_scoped.md)
- [mapi_associated_config_upsert_preserves_canonical_message_identity](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_preserves_canonical_message_identity.md)
- [mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_upsert_keeps_named_views_with_distinct_subjects.md)
- [mapi_identity_repair_removes_orphaned_checkpoint_and_config_state](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_identity_repair_removes_orphaned_checkpoint_and_config_state.md)