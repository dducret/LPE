---
type: Rust Function
title: owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
resource: crates/lpe-exchange/src/mapi/identity.rs#L1493-L1607
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests
  - functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_source_key
  - functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity
---

# Signature

`async fn owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate()`

# Calls

- [legacy_for_tests](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiIdentityCodec/legacy_for_tests.md)
- [seed_from_identity_records](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/MapiRequestIdentityScope/seed_from_identity_records.md)
- [remember_mapi_identity_with_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [serialize_folder_row_with_context](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [with_current_mapi_request_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope.md)
- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [try_mapi_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id.md)
- [mapped_mapi_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_source_key.md)
- [forget_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/forget_mapi_identity.md)