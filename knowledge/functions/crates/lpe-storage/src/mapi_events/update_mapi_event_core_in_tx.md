---
type: Rust Function
title: update_mapi_event_core_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L869-L949
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
---

# Signature

`async fn update_mapi_event_core_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, input: &UpsertClientEventInput, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [commit_mapi_event_update](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)