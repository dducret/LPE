---
type: Rust Method
title: commit_mapi_event_update
resource: crates/lpe-storage/src/mapi_events.rs#L533-L708
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_commit_input
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/mapi_events/mapi_event_identity_object_kind
  - functions/crates/lpe-storage/src/mapi_events/update_mapi_event_core_in_tx
  - functions/crates/lpe-storage/src/mapi_events/update_mapi_event_reminder_in_tx
  - functions/crates/lpe-storage/src/mapi_events/apply_mapi_event_custom_properties_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
  - functions/crates/lpe-storage/src/mapi_events/fetch_mapi_event_reminder_state_in_tx
  - functions/crates/lpe-storage/src/mapi_events/fetch_event_timestamps_in_tx
---

# Signature

`pub async fn commit_mapi_event_update( &self, input: MapiEventCommitInput, ) -> Result<MapiEventCommitOutcome>`

# Calls

- [validate_mapi_event_commit_input](../../../../../../functions/crates/lpe-storage/src/mapi_events/validate_mapi_event_commit_input.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [mapi_event_identity_object_kind](../../../../../../functions/crates/lpe-storage/src/mapi_events/mapi_event_identity_object_kind.md)
- [update_mapi_event_core_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/update_mapi_event_core_in_tx.md)
- [update_mapi_event_reminder_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/update_mapi_event_reminder_in_tx.md)
- [apply_mapi_event_custom_properties_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/apply_mapi_event_custom_properties_in_tx.md)
- [apply_mapi_event_attachment_changes_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [advance_mapi_event_version_for_lifecycle_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx.md)
- [calendar_event_affected_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)
- [fetch_mapi_event_reminder_state_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/fetch_mapi_event_reminder_state_in_tx.md)
- [fetch_event_timestamps_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/fetch_event_timestamps_in_tx.md)