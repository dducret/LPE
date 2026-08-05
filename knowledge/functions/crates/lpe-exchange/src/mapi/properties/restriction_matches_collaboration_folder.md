---
type: Rust Function
title: restriction_matches_collaboration_folder
resource: crates/lpe-exchange/src/mapi/properties.rs#L196-L203
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches
---

# Signature

`pub(in crate::mapi) fn restriction_matches_collaboration_folder( restriction: Option<&MapiRestriction>, folder: &MapiCollaborationFolder, ) -> bool`

# Calls

- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [collaboration_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)

# Called by

- [hierarchy_rows_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [hierarchy_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches.md)