---
type: Rust Module
title: attachments
resource: crates/lpe-storage/src/attachments.rs#L1-L1289
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-hashmap
  - external/anyhow-result
  - external/serde-serialize
  - external/sqlx-postgres-row
  - external/uuid-uuid
  - external/crate-blob-store-durableblobkind-postgresblobstore-putblobrequest-storedblobref-mapi-events-mapieventcustompropertyvalue-mapi-message-identity-rotate-active-mapi-message-identity-in-tx-submission-attachmentuploadinput-activesyncattachment-activesyncattachmentcontent-auditentryinput-canonicalchangecategory-jmapemail-jmapuploadblob-storage
  - external/super-normalize-attachment-content-id-supports-attachment-text-extraction
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [ClientAttachment](../../../../classes/crates/lpe-storage/src/attachments/ClientAttachment.md)
- [CalendarEventAttachment](../../../../classes/crates/lpe-storage/src/attachments/CalendarEventAttachment.md)
- [MapiEventAttachmentUpsert](../../../../classes/crates/lpe-storage/src/attachments/MapiEventAttachmentUpsert.md)
- [MapiEventAttachmentChanges](../../../../classes/crates/lpe-storage/src/attachments/MapiEventAttachmentChanges.md)
- [message_is_visible_draft](../../../../functions/crates/lpe-storage/src/attachments/Storage/message_is_visible_draft.md)
- [insert_calendar_event_attachment_in_tx](../../../../functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx.md)
- [delete_calendar_event_attachment_in_tx](../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment_in_tx.md)
- [fetch_calendar_event_attachments_in_tx](../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_event_attachments_in_tx.md)
- [apply_mapi_event_attachment_changes_in_tx](../../../../functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [fetch_calendar_event_attachments](../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_event_attachments.md)
- [fetch_calendar_attachments_for_events](../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachments_for_events.md)
- [add_calendar_event_attachment](../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [fetch_calendar_attachment_blob](../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachment_blob.md)
- [fetch_calendar_attachment_content](../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachment_content.md)
- [delete_calendar_event_attachment](../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment.md)
- [add_message_attachment](../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [delete_message_attachment](../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment.md)
- [store_attachment_blob_in_tx](../../../../functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx.md)
- [calendar_event_attachment_from_row](../../../../functions/crates/lpe-storage/src/attachments/calendar_event_attachment_from_row.md)
- [replace_attachment_custom_properties_in_tx](../../../../functions/crates/lpe-storage/src/attachments/replace_attachment_custom_properties_in_tx.md)
- [validate_mapi_event_attachment_changes](../../../../functions/crates/lpe-storage/src/attachments/validate_mapi_event_attachment_changes.md)
- [calendar_attachment_file_reference](../../../../functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference.md)
- [parse_calendar_attachment_file_reference](../../../../functions/crates/lpe-storage/src/attachments/parse_calendar_attachment_file_reference.md)
- [supports_attachment_text_extraction](../../../../functions/crates/lpe-storage/src/attachments/supports_attachment_text_extraction.md)
- [attachment_disposition](../../../../functions/crates/lpe-storage/src/attachments/attachment_disposition.md)
- [normalize_attachment_content_id](../../../../functions/crates/lpe-storage/src/attachments/normalize_attachment_content_id.md)
- [attachment_kind](../../../../functions/crates/lpe-storage/src/attachments/attachment_kind.md)
- [attachment_extension_label](../../../../functions/crates/lpe-storage/src/attachments/attachment_extension_label.md)
- [media_type_label](../../../../functions/crates/lpe-storage/src/attachments/media_type_label.md)
- [extraction_queue_scope_is_limited_to_document_text_formats](../../../../functions/crates/lpe-storage/src/attachments/extraction_queue_scope_is_limited_to_document_text_formats.md)
- [attachment_content_id_is_normalized_for_lookup](../../../../functions/crates/lpe-storage/src/attachments/attachment_content_id_is_normalized_for_lookup.md)

# Imports

- `std::collections::HashMap`
- `anyhow::Result`
- `serde::Serialize`
- `sqlx::{Postgres, Row}`
- `uuid::Uuid`
- `crate::{
    blob_store::{DurableBlobKind, PostgresBlobStore, PutBlobRequest, StoredBlobRef},
    mapi_events::MapiEventCustomPropertyValue,
    mapi_message_identity::rotate_active_mapi_message_identity_in_tx,
    submission::AttachmentUploadInput,
    ActiveSyncAttachment, ActiveSyncAttachmentContent, AuditEntryInput, CanonicalChangeCategory,
    JmapEmail, JmapUploadBlob, Storage,
}`
- `super::{normalize_attachment_content_id, supports_attachment_text_extraction}`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)