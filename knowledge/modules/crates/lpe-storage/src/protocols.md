---
type: Rust Module
title: protocols
resource: crates/lpe-storage/src/protocols.rs#L1-L1526
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-hashmap
  - external/anyhow-anyhow-result
  - external/serde-serialize
  - external/serde-json-value
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-submission-submission-attachmentuploadinput-submittedrecipientinput-auditentryinput-jmapemailrecipientrow-jmapemailrow-jmapemailsubmissionrow-messagebccrecipientrecordrow-storage-default-task-list-role
  - external/super-is-mapi-only-change-jmap-change-kind-jmap-exact-object-kind-jmap-object-replay-kinds-jmap-replay-object-id
  - external/serde-json-json
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [JmapEmailAddress](../../../../classes/crates/lpe-storage/src/protocols/JmapEmailAddress.md)
- [JmapEmail](../../../../classes/crates/lpe-storage/src/protocols/JmapEmail.md)
- [JmapEmailFollowupUpdate](../../../../classes/crates/lpe-storage/src/protocols/JmapEmailFollowupUpdate.md)
- [JmapEmailMailboxState](../../../../classes/crates/lpe-storage/src/protocols/JmapEmailMailboxState.md)
- [JmapMailObjectChange](../../../../classes/crates/lpe-storage/src/protocols/JmapMailObjectChange.md)
- [JmapStringObjectChange](../../../../classes/crates/lpe-storage/src/protocols/JmapStringObjectChange.md)
- [JmapEmailSubmission](../../../../classes/crates/lpe-storage/src/protocols/JmapEmailSubmission.md)
- [JmapImportedEmailInput](../../../../classes/crates/lpe-storage/src/protocols/JmapImportedEmailInput.md)
- [fetch_jmap_mail_change_cursor](../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_mail_change_cursor.md)
- [replay_jmap_mail_object_changes](../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_mail_object_changes.md)
- [fetch_jmap_object_change_cursor](../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_object_change_cursor.md)
- [replay_jmap_object_changes](../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes.md)
- [replay_jmap_string_object_changes](../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_string_object_changes.md)
- [expand_jmap_dependency_change](../../../../functions/crates/lpe-storage/src/protocols/Storage/expand_jmap_dependency_change.md)
- [jmap_string_replay_object_id](../../../../functions/crates/lpe-storage/src/protocols/Storage/jmap_string_replay_object_id.md)
- [task_share_type_for_collection](../../../../functions/crates/lpe-storage/src/protocols/Storage/task_share_type_for_collection.md)
- [fetch_jmap_emails](../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_emails.md)
- [fetch_jmap_emails_with_protected_bcc](../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_emails_with_protected_bcc.md)
- [fetch_visible_protected_bcc_recipients](../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_visible_protected_bcc_recipients.md)
- [update_imap_flags](../../../../functions/crates/lpe-storage/src/protocols/Storage/update_imap_flags.md)
- [expunge_imap_deleted](../../../../functions/crates/lpe-storage/src/protocols/Storage/expunge_imap_deleted.md)
- [delete_custom_jmap_email](../../../../functions/crates/lpe-storage/src/protocols/Storage/delete_custom_jmap_email.md)
- [delete_jmap_email](../../../../functions/crates/lpe-storage/src/protocols/Storage/delete_jmap_email.md)
- [delete_jmap_email_from_mailbox](../../../../functions/crates/lpe-storage/src/protocols/Storage/delete_jmap_email_from_mailbox.md)
- [fetch_jmap_draft](../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_draft.md)
- [fetch_jmap_email_submissions](../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_email_submissions.md)
- [is_mapi_only_change](../../../../functions/crates/lpe-storage/src/protocols/is_mapi_only_change.md)
- [jmap_change_kind](../../../../functions/crates/lpe-storage/src/protocols/jmap_change_kind.md)
- [jmap_exact_object_kind](../../../../functions/crates/lpe-storage/src/protocols/jmap_exact_object_kind.md)
- [jmap_replay_object_id](../../../../functions/crates/lpe-storage/src/protocols/jmap_replay_object_id.md)
- [jmap_object_replay_kinds](../../../../functions/crates/lpe-storage/src/protocols/jmap_object_replay_kinds.md)
- [summary_json_reminder_changed](../../../../functions/crates/lpe-storage/src/protocols/summary_json_reminder_changed.md)
- [jmap_replay_ignores_protocol_local_mapi_hierarchy_versions](../../../../functions/crates/lpe-storage/src/protocols/jmap_replay_ignores_protocol_local_mapi_hierarchy_versions.md)
- [jmap_replay_treats_folder_copies_as_new_objects](../../../../functions/crates/lpe-storage/src/protocols/jmap_replay_treats_folder_copies_as_new_objects.md)
- [jmap_object_replay_kinds_include_visibility_dependencies](../../../../functions/crates/lpe-storage/src/protocols/jmap_object_replay_kinds_include_visibility_dependencies.md)
- [jmap_collection_replay_maps_grant_rows_to_collection_ids](../../../../functions/crates/lpe-storage/src/protocols/jmap_collection_replay_maps_grant_rows_to_collection_ids.md)
- [jmap_collection_replay_keeps_exact_object_rows](../../../../functions/crates/lpe-storage/src/protocols/jmap_collection_replay_keeps_exact_object_rows.md)

# Imports

- `std::collections::HashMap`
- `anyhow::{anyhow, Result}`
- `serde::Serialize`
- `serde_json::Value`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::{
    submission,
    submission::{AttachmentUploadInput, SubmittedRecipientInput},
    AuditEntryInput, JmapEmailRecipientRow, JmapEmailRow, JmapEmailSubmissionRow,
    MessageBccRecipientRecordRow, Storage, DEFAULT_TASK_LIST_ROLE,
}`
- `super::{
        is_mapi_only_change, jmap_change_kind, jmap_exact_object_kind, jmap_object_replay_kinds,
        jmap_replay_object_id,
    }`
- `serde_json::json`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)