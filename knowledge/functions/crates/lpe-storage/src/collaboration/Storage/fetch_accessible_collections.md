---
type: Rust Method
title: fetch_accessible_collections
resource: crates/lpe-storage/src/collaboration.rs#L972-L1176
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/collaboration/types/shared_collection_id_for_row
  - functions/crates/lpe-storage/src/collaboration/types/shared_collection_display_name
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contact_collections
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_calendar_collections
  - functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access
---

# Signature

`async fn fetch_accessible_collections( &self, principal_account_id: Uuid, kind: CollaborationResourceKind, ) -> Result<Vec<CollaborationCollection>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [account_identity_for_id](../../../../../../functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id.md)
- [ensure_default_calendar_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [shared_collection_id_for_row](../../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_id_for_row.md)
- [shared_collection_display_name](../../../../../../functions/crates/lpe-storage/src/collaboration/types/shared_collection_display_name.md)

# Called by

- [fetch_accessible_contact_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contact_collections.md)
- [fetch_accessible_calendar_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_calendar_collections.md)
- [resolve_collection_access](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access.md)