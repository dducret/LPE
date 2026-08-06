---
type: Rust Function
title: mapi_identity_requests_for_mailboxes
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L992-L996
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
---

# Signature

`pub(in crate::mapi) fn mapi_identity_requests_for_mailboxes( mailboxes: &[JmapMailbox], ) -> Vec<MapiIdentityRequest>`

# Calls

- [mapi_folder_identity_requests](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/mapi_folder_identity_requests.md)

# Called by

- [load_mapi_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)