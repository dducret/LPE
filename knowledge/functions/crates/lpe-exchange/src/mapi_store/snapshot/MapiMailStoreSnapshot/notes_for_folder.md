---
type: Rust Method
title: notes_for_folder
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1168-L1173
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/tests/note_and_journal_message_handles_serialize_object_properties
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders
---

# Signature

`pub(crate) fn notes_for_folder(&self, folder_id: u64) -> Vec<&MapiNote>`

# Called by

- [note_and_journal_message_handles_serialize_object_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/note_and_journal_message_handles_serialize_object_properties.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/snapshot_projects_canonical_notes_and_journal_into_default_mapi_folders.md)