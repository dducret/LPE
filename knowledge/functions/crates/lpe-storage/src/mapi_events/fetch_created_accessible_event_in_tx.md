---
type: Rust Function
title: fetch_created_accessible_event_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L1119-L1194
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
---

# Signature

`async fn fetch_created_accessible_event_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, event_id: Uuid, collection_id: String, owner_email: String, owner_display_name: String, rights: CollaborationRights, ) -> Result<AccessibleEvent>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [create_mapi_event](../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)