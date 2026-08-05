---
type: Rust Function
title: rekey_active_event_identities_in_tx
resource: crates/lpe-storage/src/collaboration/deleted_events.rs#L193-L411
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/collaboration/deleted_events/checked_positive_u64
  - functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list
  - functions/crates/lpe-storage/src/mapi_events/mapi_change_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items
---

# Signature

`async fn rekey_active_event_identities_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, event_id: Uuid, imported_identity: Option<&MapiEventImportedMoveIdentity>, ) -> Result<Vec<MapiEventIdentityMove>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [checked_positive_u64](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/checked_positive_u64.md)
- [allocate_global_counter_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/allocate_global_counter_in_tx.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [merge_predecessor_change_list](../../../../../../functions/crates/lpe-storage/src/mapi_events/merge_predecessor_change_list.md)
- [mapi_change_key](../../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_change_key.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [move_accessible_event_to_deleted_items](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items.md)