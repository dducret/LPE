---
type: Rust Function
title: special_hierarchy_row_matches
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L524-L532
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches
---

# Signature

`pub(super) fn special_hierarchy_row_matches( folder_id: u64, restriction: Option<&MapiRestriction>, mailbox_guid: Uuid, ) -> bool`

# Calls

- [restriction_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [special_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)

# Called by

- [hierarchy_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [hierarchy_row_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches.md)