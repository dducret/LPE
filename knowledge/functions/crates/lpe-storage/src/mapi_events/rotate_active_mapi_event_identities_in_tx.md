---
type: Rust Function
title: rotate_active_mapi_event_identities_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L1240-L1246
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx
---

# Signature

`async fn rotate_active_mapi_event_identities_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, event_id: Uuid, ) -> Result<Vec<EventIdentityVersion>>`

# Calls

- [rotate_mapi_event_identities_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx.md)

# Called by

- [advance_mapi_event_version_for_lifecycle_in_tx](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx.md)