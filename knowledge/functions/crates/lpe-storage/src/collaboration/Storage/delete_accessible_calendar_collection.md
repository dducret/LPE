---
type: Rust Method
title: delete_accessible_calendar_collection
resource: crates/lpe-storage/src/collaboration.rs#L405-L472
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
---

# Signature

`pub async fn delete_accessible_calendar_collection( &self, principal_account_id: Uuid, collection_id: &str, ) -> Result<()>`

# Calls

- [resolve_collection_access](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_calendar_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx.md)
- [move_calendar_events_to_collection_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [insert_collaboration_tombstone_in_tx](../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_tombstone_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)