---
type: Rust Method
title: upsert_client_event_in_calendar
resource: crates/lpe-storage/src/workspace.rs#L630-L829
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/workspace/event_update_is_unchanged
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
  - functions/crates/lpe-storage/src/workspace/map_event
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_event
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event
---

# Signature

`pub(crate) async fn upsert_client_event_in_calendar( &self, input: UpsertClientEventInput, calendar_id: Option<Uuid>, ) -> Result<ClientEvent>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [event_update_is_unchanged](../../../../../../functions/crates/lpe-storage/src/workspace/event_update_is_unchanged.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_default_calendar_in_tx](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/ensure_default_calendar_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [advance_calendar_event_version_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx.md)
- [calendar_event_affected_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)
- [map_event](../../../../../../functions/crates/lpe-storage/src/workspace/map_event.md)

# Called by

- [create_accessible_event](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_event.md)
- [update_accessible_event](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event.md)
- [upsert_client_event](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event.md)