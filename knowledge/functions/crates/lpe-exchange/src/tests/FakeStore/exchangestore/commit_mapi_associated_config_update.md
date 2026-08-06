---
type: Rust Method
title: commit_mapi_associated_config_update
resource: crates/lpe-exchange/src/tests/mod.rs#L10610-L10700
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
---

# Signature

`fn commit_mapi_associated_config_update<'a>( &'a self, input: crate::store::UpsertMapiAssociatedConfigInput, ) -> StoreFuture<'a, crate::store::MapiAssociatedConfigCommit>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [test_merge_mapi_predecessor_change_lists](../../../../../../../functions/crates/lpe-exchange/src/tests/test_merge_mapi_predecessor_change_lists.md)
- [upsert_mapi_associated_config](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/upsert_mapi_associated_config.md)

# Called by

- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)