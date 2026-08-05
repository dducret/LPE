---
type: Rust Method
title: move_accessible_event_to_deleted_items
resource: crates/lpe-storage/src/collaboration/deleted_events.rs#L29-L190
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx
  - functions/crates/lpe-storage/src/change/Storage/insert_collaboration_move_tombstone_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
---

# Signature

`pub async fn move_accessible_event_to_deleted_items( &self, principal_account_id: Uuid, event_id: Uuid, imported_identity: Option<MapiEventImportedMoveIdentity>, ) -> Result<MoveAccessibleEventToDeletedItemsResult>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [calendar_event_affected_principals_in_tx](../../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx.md)
- [insert_collaboration_move_tombstone_in_tx](../../../../../../../functions/crates/lpe-storage/src/change/Storage/insert_collaboration_move_tombstone_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [rekey_active_event_identities_in_tx](../../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/rekey_active_event_identities_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)