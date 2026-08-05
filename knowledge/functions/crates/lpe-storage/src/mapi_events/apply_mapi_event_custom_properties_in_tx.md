---
type: Rust Function
title: apply_mapi_event_custom_properties_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L1003-L1059
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
---

# Signature

`async fn apply_mapi_event_custom_properties_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, principal_account_id: Uuid, event_id: Uuid, upserts: &[MapiEventCustomPropertyValue], deletes: &[u32], ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)