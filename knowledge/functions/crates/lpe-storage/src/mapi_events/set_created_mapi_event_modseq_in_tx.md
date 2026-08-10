---
type: Rust Function
title: set_created_mapi_event_modseq_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L1070-L1117
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
---

# Signature

`async fn set_created_mapi_event_modseq_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, calendar_id: Uuid, event_id: Uuid, modseq: i64, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)