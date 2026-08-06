---
type: Rust Function
title: expected_folder_type_for_debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L729-L751
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/advertised_special_search_folder_for_debug
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
---

# Signature

`pub(in crate::mapi) fn expected_folder_type_for_debug( folder_id: u64, mailbox: Option<&JmapMailbox>, search_folder_found: bool, ) -> (&'static str, Option<u32>)`

# Calls

- [advertised_special_search_folder_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/advertised_special_search_folder_for_debug.md)
- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)

# Called by

- [format_folder_type_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)