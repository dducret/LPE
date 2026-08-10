---
type: Rust Function
title: event_fixture
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L125-L284
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/tests/mapi_event_commit/change_key
  called_by:
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_fixture_drop_cleans_temporary_schema
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_one_atomic_version
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_subject_and_attachment_with_one_change
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_attachment_failure_rolls_back_parent_and_blob
  - functions/crates/lpe-storage/tests/mapi_event_commit/canonical_event_writer_advances_the_persisted_mapi_version
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rejects_stale_version_unless_force_save
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rolls_back_when_change_number_allocation_fails
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_create_rolls_back_every_artifact_and_retry_creates_one_event
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic
  - functions/crates/lpe-storage/tests/mapi_event_commit/delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties
  - functions/crates/lpe-storage/tests/mapi_event_commit/calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity
  - functions/crates/lpe-storage/tests/mapi_event_commit/event_delete_preserves_custom_shared_calendar_tombstone_scope
---

# Signature

`async fn event_fixture() -> Result<Option<EventFixture>>`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [change_key](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/change_key.md)

# Called by

- [mapi_event_fixture_drop_cleans_temporary_schema](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_fixture_drop_cleans_temporary_schema.md)
- [mapi_event_commit_persists_one_atomic_version](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_one_atomic_version.md)
- [mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_message_mutations_rotate_durable_mapi_version_without_rekeying_identity.md)
- [mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock.md)
- [mapi_event_commit_persists_subject_and_attachment_with_one_change](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_subject_and_attachment_with_one_change.md)
- [mapi_event_attachment_failure_rolls_back_parent_and_blob](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_attachment_failure_rolls_back_parent_and_blob.md)
- [canonical_event_writer_advances_the_persisted_mapi_version](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/canonical_event_writer_advances_the_persisted_mapi_version.md)
- [mapi_event_commit_rejects_stale_version_unless_force_save](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rejects_stale_version_unless_force_save.md)
- [mapi_event_commit_rolls_back_when_change_number_allocation_fails](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rolls_back_when_change_number_allocation_fails.md)
- [mapi_event_create_rolls_back_every_artifact_and_retry_creates_one_event](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_create_rolls_back_every_artifact_and_retry_creates_one_event.md)
- [microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_event_keeps_client_xids_and_allocates_server_cn.md)
- [microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_calendar_move_is_atomic_and_keeps_destination_xids.md)
- [microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic.md)
- [delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/delegated_mapi_event_create_uses_owner_scope_for_event_and_custom_properties.md)
- [calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity.md)
- [event_delete_preserves_custom_shared_calendar_tombstone_scope](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/event_delete_preserves_custom_shared_calendar_tombstone_scope.md)