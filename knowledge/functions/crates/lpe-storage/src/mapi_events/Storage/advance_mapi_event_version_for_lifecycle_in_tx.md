---
type: Rust Method
title: advance_mapi_event_version_for_lifecycle_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L722-L783
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/mapi_event_identity_object_kind
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_events/rotate_active_mapi_event_identities_in_tx
  - functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx
---

# Signature

`async fn advance_mapi_event_version_for_lifecycle_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, event_id: Uuid, modseq: i64, lifecycle_state: &str, imported_principal_account_id: Option<Uuid>, imported_identity: Option<&MapiEventImportedIdentity>, ) -> Result<Vec<EventIdentityVersion>>`

# Calls

- [mapi_event_identity_object_kind](../../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_event_identity_object_kind.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [rotate_active_mapi_event_identities_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_active_mapi_event_identities_in_tx.md)
- [rotate_mapi_event_identities_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/rotate_mapi_event_identities_in_tx.md)

# Called by

- [commit_mapi_event_update](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)
- [advance_calendar_event_version_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx.md)