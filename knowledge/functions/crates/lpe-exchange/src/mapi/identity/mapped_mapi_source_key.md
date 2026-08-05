---
type: Rust Function
title: mapped_mapi_source_key
resource: crates/lpe-exchange/src/mapi/identity.rs#L720-L732
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/current_mapi_request_identities
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
---

# Signature

`pub(crate) fn mapped_mapi_source_key(canonical_id: &Uuid) -> Option<Vec<u8>>`

# Calls

- [current_mapi_request_identities](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/current_mapi_request_identities.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [source_key_for_uuid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)