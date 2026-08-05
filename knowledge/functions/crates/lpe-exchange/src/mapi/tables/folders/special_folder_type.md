---
type: Rust Function
title: special_folder_type
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L578-L588
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
---

# Signature

`pub(super) fn special_folder_type(folder_id: u64) -> u32`

# Called by

- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [special_folder_property_value_with_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)