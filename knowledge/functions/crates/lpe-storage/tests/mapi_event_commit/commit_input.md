---
type: Rust Function
title: commit_input
resource: crates/lpe-storage/tests/mapi_event_commit.rs#L348-L369
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/mapi_event_commit/updated_event
  called_by:
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_one_atomic_version
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_subject_and_attachment_with_one_change
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_attachment_failure_rolls_back_parent_and_blob
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rejects_stale_version_unless_force_save
  - functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rolls_back_when_change_number_allocation_fails
  - functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic
  - functions/crates/lpe-storage/tests/mapi_event_commit/calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity
---

# Signature

`fn commit_input(fixture: &EventFixture, title: &str) -> MapiEventCommitInput`

# Calls

- [updated_event](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/updated_event.md)

# Called by

- [mapi_event_commit_persists_one_atomic_version](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_one_atomic_version.md)
- [mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_updated_at_advances_after_waiting_for_a_row_lock.md)
- [mapi_event_commit_persists_subject_and_attachment_with_one_change](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_persists_subject_and_attachment_with_one_change.md)
- [mapi_event_attachment_failure_rolls_back_parent_and_blob](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_attachment_failure_rolls_back_parent_and_blob.md)
- [mapi_event_commit_rejects_stale_version_unless_force_save](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rejects_stale_version_unless_force_save.md)
- [mapi_event_commit_rolls_back_when_change_number_allocation_fails](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/mapi_event_commit_rolls_back_when_change_number_allocation_fails.md)
- [microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/microsoft_oxcfxics_imported_deleted_event_update_keeps_identity_and_is_atomic.md)
- [calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity](../../../../../functions/crates/lpe-storage/tests/mapi_event_commit/calendar_event_move_to_deleted_items_preserves_canonical_content_and_rekeys_identity.md)