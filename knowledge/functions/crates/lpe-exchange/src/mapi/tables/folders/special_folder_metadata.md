---
type: Rust Function
title: special_folder_metadata
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L472-L576
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_parent_id
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxosfld_special_folder_metadata_covers_bounded_list
---

# Signature

`pub(super) fn special_folder_metadata(folder_id: u64) -> (&'static str, u64, &'static str, bool)`

# Called by

- [folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class.md)
- [advertised_special_folder_id_for_create](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/advertised_special_folder_id_for_create.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [hierarchy_row_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_display_name.md)
- [hierarchy_row_parent_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_parent_id.md)
- [special_folder_property_value_with_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)
- [microsoft_oxosfld_special_folder_metadata_covers_bounded_list](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxosfld_special_folder_metadata_covers_bounded_list.md)