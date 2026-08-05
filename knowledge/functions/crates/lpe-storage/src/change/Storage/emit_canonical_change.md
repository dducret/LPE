---
type: Rust Method
title: emit_canonical_change
resource: crates/lpe-storage/src/change.rs#L190-L230
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_change
  - functions/crates/lpe-storage/src/change/Storage/emit_mail_delegation_change
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change
  - functions/crates/lpe-storage/src/change/Storage/emit_collaboration_grant_change
  - functions/crates/lpe-storage/src/change/Storage/emit_task_access_change
  - functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change
  - functions/crates/lpe-storage/src/conversation_actions/Storage/upsert_conversation_action
  - functions/crates/lpe-storage/src/conversation_actions/Storage/delete_conversation_action
  - functions/crates/lpe-storage/src/notes_journal/Storage/upsert_client_note
  - functions/crates/lpe-storage/src/notes_journal/Storage/delete_client_note
  - functions/crates/lpe-storage/src/notes_journal/Storage/upsert_journal_entry
  - functions/crates/lpe-storage/src/notes_journal/Storage/delete_journal_entry
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected
  - functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change
---

# Signature

`pub(crate) async fn emit_canonical_change( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, category: CanonicalChangeCategory, principal_account_ids: &[Uuid], account_ids: &[Uuid], ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [emit_mail_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_change.md)
- [emit_mail_delegation_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_mail_delegation_change.md)
- [emit_collaboration_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_change.md)
- [emit_collaboration_grant_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_collaboration_grant_change.md)
- [emit_task_access_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_task_access_change.md)
- [emit_account_scoped_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_account_scoped_change.md)
- [upsert_conversation_action](../../../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/upsert_conversation_action.md)
- [delete_conversation_action](../../../../../../functions/crates/lpe-storage/src/conversation_actions/Storage/delete_conversation_action.md)
- [upsert_client_note](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_client_note.md)
- [delete_client_note](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/delete_client_note.md)
- [upsert_journal_entry](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/upsert_journal_entry.md)
- [delete_journal_entry](../../../../../../functions/crates/lpe-storage/src/notes_journal/Storage/delete_journal_entry.md)
- [record_public_folder_change_with_extra_affected](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_change_with_extra_affected.md)
- [record_public_folder_private_change](../../../../../../functions/crates/lpe-storage/src/public_folders/changes/Storage/record_public_folder_private_change.md)