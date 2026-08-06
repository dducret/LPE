---
type: Rust Function
title: serialize_hierarchy_property_row
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L820-L860
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_is_present
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_row_modified
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection
---

# Signature

`pub(super) fn serialize_hierarchy_property_row( row: HierarchyRow<'_>, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, columns: &[u32], mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [hierarchy_row_property_is_present](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_is_present.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)
- [write_query_rows_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_query_rows_property_row.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [hierarchy_table_row_modified](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_row_modified.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/hierarchy_property_row_matches_exchange_xview_and_folder_flags_projection.md)