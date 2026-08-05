---
type: Rust Function
title: fetch_mapi_event_reminder_state_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L1187-L1225
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
---

# Signature

`async fn fetch_mapi_event_reminder_state_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, event_id: Uuid, ) -> Result<MapiEventReminderState>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)