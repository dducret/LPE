---
type: Rust Module
title: runtime_schema_drift
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L1-L6132
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-env-str-fromstr
  - external/anyhow-context-result
  - external/lpe-domain-inbounddeliveryrequest
  - external/lpe-storage-mapi-store-identity-mapi-store-id-mapi-xid-attachmentuploadinput-auditentryinput-cancelsubmissionresult-collaborationgrantinput-collaborationresourcekind-createpublicfoldertreeinput-jmapimportedemailinput-jmapmailboxcreateinput-jmapmailboxupdateinput-managedretentionfoldercreateinput-newaccount-newdomain-newmailbox-newpsttransferjob-publicfolderperuserstatepatch-publicfolderpermissioninput-publicfolderreplicainput-reminderquery-senderdelegationgrantinput-senderdelegationright-storage-submitmessageinput-submittedmessage-submittedrecipientinput-upsertclienteventinput-upsertclientnoteinput-upsertjournalentryinput-upsertpublicfolderiteminput-upsertsearchfolderinput
  - external/sqlx-postgres-pgconnectoptions-pgpooloptions-pgrow-pgpool-row
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [RuntimeFixture](../../../../classes/crates/lpe-storage/tests/runtime_schema_drift/RuntimeFixture.md)
- [schema_sql_matches_representative_runtime_paths_when_database_is_enabled](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/schema_sql_matches_representative_runtime_paths_when_database_is_enabled.md)
- [run_runtime_drift_validation](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
- [collect](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/collect.md)
- [assert_schema_metadata](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/assert_schema_metadata.md)
- [seed_platform_tenant](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/seed_platform_tenant.md)
- [exercise_blob_reference_constraints](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_blob_reference_constraints.md)
- [insert_blob](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/insert_blob.md)
- [expect_constraint_failure](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure.md)
- [expect_anyhow_failure](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_anyhow_failure.md)
- [jmap_create_input](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/jmap_create_input.md)
- [hex64](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/hex64.md)
- [exercise_admin_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_path.md)
- [seed_mailbox_fixture](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/seed_mailbox_fixture.md)
- [exercise_mapi_local_replica_range_constraints](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_local_replica_range_constraints.md)
- [exercise_mapi_outlook_cache_fidelity_constraints](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_outlook_cache_fidelity_constraints.md)
- [exercise_mailbox_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_path.md)
- [exercise_inbound_mime_canonical_body_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_inbound_mime_canonical_body_path.md)
- [exercise_notes_journal_reminder_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_notes_journal_reminder_path.md)
- [seed_reminder_rows](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/seed_reminder_rows.md)
- [exercise_mailbox_name_policy_storage_guards](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_name_policy_storage_guards.md)
- [exercise_managed_retention_folder_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_managed_retention_folder_path.md)
- [exercise_change_log_cursor_constraints](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_change_log_cursor_constraints.md)
- [exercise_mapi_special_folder_alias_constraints](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_special_folder_alias_constraints.md)
- [insert_mapi_special_folder_alias](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/insert_mapi_special_folder_alias.md)
- [mapi_source_key](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/mapi_source_key.md)
- [exercise_submission_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_path.md)
- [exercise_submission_cancellation_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_cancellation_path.md)
- [exercise_jmap_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_jmap_path.md)
- [exercise_index_plan_paths](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_index_plan_paths.md)
- [explain_rows](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/explain_rows.md)
- [assert_plan_uses_index](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/assert_plan_uses_index.md)
- [exercise_custom_calendar_grant_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path.md)
- [runtime_calendar_event_input](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/runtime_calendar_event_input.md)
- [exercise_activesync_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_activesync_path.md)
- [exercise_pst_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_pst_path.md)
- [exercise_admin_dashboard_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_admin_dashboard_path.md)
- [exercise_mailbox_move_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mailbox_move_path.md)
- [exercise_mapi_cross_protocol_interoperability_gate](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_cross_protocol_interoperability_gate.md)
- [exercise_canonical_identity_allocation](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_identity_allocation.md)
- [exercise_canonical_search_folder_and_rule_replay](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_canonical_search_folder_and_rule_replay.md)
- [exercise_public_folder_replica_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_replica_path.md)
- [exercise_public_folder_permission_replay_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_permission_replay_path.md)
- [exercise_public_folder_per_user_replay_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_public_folder_per_user_replay_path.md)
- [exercise_mapi_delete_cross_protocol_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_delete_cross_protocol_path.md)
- [exercise_mapi_trash_purge_cross_protocol_path](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_trash_purge_cross_protocol_path.md)
- [exercise_mapi_trash_purge_retention_guard](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_mapi_trash_purge_retention_guard.md)
- [audit](../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/audit.md)

# Imports

- `std::{env, str::FromStr}`
- `anyhow::{Context, Result}`
- `lpe_domain::InboundDeliveryRequest`
- `lpe_storage::{
    mapi_store_identity::{mapi_store_id, mapi_xid},
    AttachmentUploadInput, AuditEntryInput, CancelSubmissionResult, CollaborationGrantInput,
    CollaborationResourceKind, CreatePublicFolderTreeInput, JmapImportedEmailInput,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, ManagedRetentionFolderCreateInput, NewAccount,
    NewDomain, NewMailbox, NewPstTransferJob, PublicFolderPerUserStatePatch,
    PublicFolderPermissionInput, PublicFolderReplicaInput, ReminderQuery,
    SenderDelegationGrantInput, SenderDelegationRight, Storage, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput, UpsertClientEventInput, UpsertClientNoteInput,
    UpsertJournalEntryInput, UpsertPublicFolderItemInput, UpsertSearchFolderInput,
}`
- `sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
    PgPool, Row,
}`
- `uuid::Uuid`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)