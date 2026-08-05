---
type: Rust Function
title: serialize_journal_entry_row
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L238-L252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn serialize_journal_entry_row( entry: &JournalEntry, item_id: u64, folder_id: u64, columns: &[u32], ) -> Vec<u8>`

# Calls

- [journal_entry_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [serialize_pending_journal_entry_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_journal_entry_row.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)