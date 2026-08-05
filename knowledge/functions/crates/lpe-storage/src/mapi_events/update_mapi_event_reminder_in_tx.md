---
type: Rust Function
title: update_mapi_event_reminder_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L951-L995
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/reminder_patch_has_changes
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
---

# Signature

`async fn update_mapi_event_reminder_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, event_id: Uuid, reminder: &MapiEventReminderPatch, ) -> Result<()>`

# Calls

- [reminder_patch_has_changes](../../../../../functions/crates/lpe-storage/src/mapi_events/reminder_patch_has_changes.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)