---
type: Rust Module
title: mapi_event_commit
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L1-L2920
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-env-str-fromstr-sync-oncelock-time-duration
  - external/anyhow-context-result
  - external/lpe-storage-attachmentuploadinput-auditentryinput-jmapemailfollowupupdate-mapieventattachmentchanges-mapieventattachmentupsert-mapieventcommitinput-mapieventcommitoutcome-mapieventcreateinput-mapieventcustompropertyvalue-mapieventimportedidentity-mapieventimportedmoveidentity-mapieventreminderpatch-storage-submitmessageinput-submittedrecipientinput-upsertclienteventinput
  - external/sqlx-postgres-pgconnectoptions-pgpooloptions-pgpool-row
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [database_test_lock](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/database_test_lock.md)
- [EventFixture](../../../../classes/crates/lpe-storage/tests/mapi_event_commit/EventFixture.md)
- [TestSchemaCleanup](../../../../classes/crates/lpe-storage/tests/mapi_event_commit/TestSchemaCleanup.md)
- [armed](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/TestSchemaCleanup/armed.md)
- [disarm](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/TestSchemaCleanup/disarm.md)
- [drop](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/TestSchemaCleanup/drop/drop.md)
- [cleanup](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/EventFixture/cleanup.md)
- [event_fixture](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_fixture.md)
- [reserve_imported_event_range](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/reserve_imported_event_range.md)
- [updated_event](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/updated_event.md)
- [commit_input](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/commit_input.md)
- [create_input](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/create_input.md)
- [attachment_upsert](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/attachment_upsert.md)
- [mapi_event_fixture_drop_cleans_temporary_schema](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_fixture_drop_cleans_temporary_schema.md)
- [mapi_event_commit_persists_one_atomic_version](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_one_atomic_version.md)
- [mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity.md)
- [mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock.md)
- [mapi_event_commit_persists_subject_and_attachment_with_one_change](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_subject_and_attachment_with_one_change.md)
- [mapi_event_attachment_failure_rolls_back_parent_and_blob](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_attachment_failure_rolls_back_parent_and_blob.md)
- [canonical_event_writer_advances_the_persisted_mapi_version](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/canonical_event_writer_advances_the_persisted_mapi_version.md)
- [mapi_event_commit_rejects_stale_version_unless_force_save](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rejects_stale_version_unless_force_save.md)
- [mapi_event_commit_rolls_back_when_change_number_allocation_fails](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rolls_back_when_change_number_allocation_fails.md)
- [mapi_event_create_rolls_back_every_artifact_and_retry_creates_one_event](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_create_rolls_back_every_artifact_and_retry_creates_one_event.md)
- [microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn.md)
- [microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids.md)
- [microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic.md)
- [delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties.md)
- [calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity.md)
- [event_delete_preserves_custom_shared_calendar_tombstone_scope](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_delete_preserves_custom_shared_calendar_tombstone_scope.md)
- [mapi_store_id](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_store_id.md)
- [change_key](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/change_key.md)
- [predecessor_change_list](../../../../functions/crates/lpe-storage/tests/mapi_event_commit/predecessor_change_list.md)

# Imports

- `std::{env, str::FromStr, sync::OnceLock, time::Duration}`
- `anyhow::{Context, Result}`
- `lpe_storage::{
    AttachmentUploadInput, AuditEntryInput, JmapEmailFollowupUpdate, MapiEventAttachmentChanges,
    MapiEventAttachmentUpsert, MapiEventCommitInput, MapiEventCommitOutcome, MapiEventCreateInput,
    MapiEventCustomPropertyValue, MapiEventImportedIdentity, MapiEventImportedMoveIdentity,
    MapiEventReminderPatch, Storage, SubmitMessageInput, SubmittedRecipientInput,
    UpsertClientEventInput,
}`
- `sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Row,
}`
- `uuid::Uuid`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)