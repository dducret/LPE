---
type: Rust Method
title: commit_mapi_associated_config_import
resource: crates/lpe-exchange/src/tests/mod.rs#L10445-L10607
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable
  - functions/crates/lpe-exchange/src/tests/mapi_associated_config_snapshot_repair_preserves_calendar_virtual_parent
---

# Signature

`fn commit_mapi_associated_config_import<'a>( &'a self, input: crate::store::CommitMapiAssociatedConfigImportInput, ) -> StoreFuture<'a, crate::store::MapiAssociatedConfigImportCommit>`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [test_mapi_pcl_includes_change_key](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_pcl_includes_change_key.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [upsert_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config.md)

# Called by

- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_import_assigns_missing_creation_and_keeps_it_stable.md)
- [mapi_associated_config_snapshot_repair_preserves_calendar_virtual_parent](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_associated_config_snapshot_repair_preserves_calendar_virtual_parent.md)