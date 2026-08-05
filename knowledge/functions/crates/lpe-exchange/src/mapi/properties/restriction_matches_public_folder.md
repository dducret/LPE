---
type: Rust Function
title: restriction_matches_public_folder
resource: crates/lpe-exchange/src/mapi/properties.rs#L205-L212
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches
---

# Signature

`pub(in crate::mapi) fn restriction_matches_public_folder( restriction: Option<&MapiRestriction>, folder: &MapiPublicFolder, ) -> bool`

# Calls

- [restriction_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [public_folder_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)

# Called by

- [hierarchy_rows_excluding_deleted](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [hierarchy_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_matches.md)