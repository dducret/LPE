---
type: Rust Function
title: fetch_event_updated_at_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L1374-L1396
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
---

# Signature

`async fn fetch_event_updated_at_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, event_id: Uuid, ) -> Result<String>`

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)