---
type: Rust Function
title: dynamic_contact_folder_exposes_only_persisted_associated_config
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L1157-L1197
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder
---

# Signature

`fn dynamic_contact_folder_exposes_only_persisted_associated_config()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [collaboration_folder_identity_canonical_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/collaboration_folder_identity_canonical_id.md)
- [associated_config_messages_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_messages_for_folder.md)