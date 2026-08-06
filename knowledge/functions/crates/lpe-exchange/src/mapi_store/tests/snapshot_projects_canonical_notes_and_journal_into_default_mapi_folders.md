---
type: Rust Function
title: snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L2620-L2681
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_notes_and_journal
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder
---

# Signature

`fn snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [with_notes_and_journal](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_notes_and_journal.md)
- [notes_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/notes_for_folder.md)
- [journal_entries_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entries_for_folder.md)