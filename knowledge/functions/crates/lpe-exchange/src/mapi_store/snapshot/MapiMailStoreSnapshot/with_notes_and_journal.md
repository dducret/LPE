---
type: Rust Method
title: with_notes_and_journal
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L700-L724
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/tests/note_and_journal_message_handles_serialize_object_properties
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders
---

# Signature

`pub(crate) fn with_notes_and_journal( mut self, notes: Vec<ClientNote>, journal_entries: Vec<JournalEntry>, ) -> Self`

# Called by

- [note_and_journal_message_handles_serialize_object_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/note_and_journal_message_handles_serialize_object_properties.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders.md)