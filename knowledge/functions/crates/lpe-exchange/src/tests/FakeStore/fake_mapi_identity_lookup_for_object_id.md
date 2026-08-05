---
type: Rust Method
title: fake_mapi_identity_lookup_for_object_id
resource: crates/lpe-exchange/src/tests/mod.rs#L4443-L4604
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_object_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_source_keys
---

# Signature

`fn fake_mapi_identity_lookup_for_object_id( &self, object_id: u64, ) -> Option<MapiIdentityLookupRecord>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [legacy_migration_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)
- [reserved_folder_counter_for_role](../../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role.md)

# Called by

- [fetch_mapi_identities_by_object_ids](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids.md)
- [fetch_mapi_object_ids_for_deleted_changes](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_object_ids_for_deleted_changes.md)
- [fetch_mapi_identities_by_source_keys](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_source_keys.md)