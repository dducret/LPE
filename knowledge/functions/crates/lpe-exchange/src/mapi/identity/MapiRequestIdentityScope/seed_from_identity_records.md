---
type: Rust Method
title: seed_from_identity_records
resource: crates/lpe-exchange/src/mapi/identity.rs#L86-L105
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/is_special_canonical_id
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/remember
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/from_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot
---

# Signature

`pub(crate) fn seed_from_identity_records( &self, records: &[MapiIdentityRecord], codec: &MapiIdentityCodec, )`

# Calls

- [is_special_canonical_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/is_special_canonical_id.md)
- [logical_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/logical_object_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [remember](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/remember.md)

# Called by

- [from_identity_records](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/from_identity_records.md)
- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [finalize_mapi_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot.md)