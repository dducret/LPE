---
type: Rust Function
title: mapi_collaboration_folder_identity_ids_for_account
resource: crates/lpe-exchange/src/store/storage_impl/mapi_helpers.rs#L495-L516
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests
---

# Signature

`async fn mapi_collaboration_folder_identity_ids_for_account( storage: &Storage, account_id: Uuid, ) -> Result<Vec<Uuid>>`

# Calls

- [collaboration_folder_identity_requests](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_requests.md)