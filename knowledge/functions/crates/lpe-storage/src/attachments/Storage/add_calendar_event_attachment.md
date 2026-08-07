---
type: Rust Method
title: add_calendar_event_attachment
resource: crates/lpe-storage/src/attachments.rs#L448-L593
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx
  - functions/crates/lpe-storage/src/attachments/attachment_disposition
  - functions/crates/lpe-storage/src/attachments/normalize_attachment_content_id
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
  - functions/crates/lpe-storage/src/shared/Storage/insert_audit
  - functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference
---

# Signature

`pub async fn add_calendar_event_attachment( &self, account_id: Uuid, event_id: Uuid, attachment: AttachmentUploadInput, audit: AuditEntryInput, ) -> Result<Option<CalendarEventAttachment>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [load_account_domain_id_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [store_attachment_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx.md)
- [attachment_disposition](../../../../../../functions/crates/lpe-storage/src/attachments/attachment_disposition.md)
- [normalize_attachment_content_id](../../../../../../functions/crates/lpe-storage/src/attachments/normalize_attachment_content_id.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [advance_calendar_event_version_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx.md)
- [calendar_event_affected_principals_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/calendar_event_affected_principals_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)
- [insert_audit](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_audit.md)
- [calendar_attachment_file_reference](../../../../../../functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference.md)