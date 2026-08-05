---
type: Rust Method
title: fetch_mapi_object_ids_for_deleted_changes
resource: crates/lpe-exchange/src/tests/mod.rs#L6729-L6756
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/deleted_special_object_ids_for_folder
---

# Signature

`fn fetch_mapi_object_ids_for_deleted_changes<'a>( &'a self, _account_id: Uuid, object_kind: MapiIdentityObjectKind, canonical_ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<u64>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [fake_mapi_identity_lookup_for_object_id](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/fake_mapi_identity_lookup_for_object_id.md)

# Called by

- [mapi_object_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes.md)
- [deleted_special_object_ids_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/deleted_special_object_ids_for_folder.md)