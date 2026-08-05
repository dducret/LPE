---
type: Rust Function
title: extended_folder_flags_for_folder
resource: crates/lpe-exchange/src/mapi/properties.rs#L660-L667
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_search_folder
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxocfg_todo_search_folder_flags_include_required_version
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number
---

# Signature

`pub(in crate::mapi) fn extended_folder_flags_for_folder(folder_id: u64) -> Vec<u8>`

# Calls

- [extended_folder_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags.md)

# Called by

- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [extended_folder_flags_for_search_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_search_folder.md)
- [microsoft_oxocfg_todo_search_folder_flags_include_required_version](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxocfg_todo_search_folder_flags_include_required_version.md)
- [special_folder_property_value_with_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value_with_change_number.md)