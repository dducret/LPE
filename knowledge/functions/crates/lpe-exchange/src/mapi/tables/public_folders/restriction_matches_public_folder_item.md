---
type: Rust Function
title: restriction_matches_public_folder_item
resource: crates/lpe-exchange/src/mapi/tables/public_folders.rs#L160-L167
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/public_folder_item_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn restriction_matches_public_folder_item( restriction: Option<&MapiRestriction>, item: &MapiPublicFolderItem, ) -> bool`

# Calls

- [restriction_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [public_folder_item_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/public_folder_item_property_value.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)