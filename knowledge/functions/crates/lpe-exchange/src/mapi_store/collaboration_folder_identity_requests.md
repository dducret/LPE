---
type: Rust Function
title: collaboration_folder_identity_requests
resource: crates/lpe-exchange/src/mapi_store.rs#L873-L905
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/mapi_identity_requests
  - functions/crates/lpe-exchange/src/mapi_store/tests/collaboration_folder_identity_requests_cover_custom_and_shared_collections
  - functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_collaboration_folder_identity_ids_for_account
---

# Signature

`pub(crate) fn collaboration_folder_identity_requests( contact_collections: &[CollaborationCollection], calendar_collections: &[CollaborationCollection], task_collections: &[CollaborationCollection], ) -> Vec<MapiIdentityRequest>`

# Calls

- [collaboration_folder_identity_canonical_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [mapi_identity_requests](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_identity_requests.md)
- [collaboration_folder_identity_requests_cover_custom_and_shared_collections](../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/collaboration_folder_identity_requests_cover_custom_and_shared_collections.md)
- [mapi_collaboration_folder_identity_ids_for_account](../../../../../functions/crates/lpe-exchange/src/store/storage_impl/mapi_helpers/mapi_collaboration_folder_identity_ids_for_account.md)