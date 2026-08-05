---
type: Rust Function
title: special_folder_alias
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L357-L370
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/indexed_special_folder_aliases
---

# Signature

`fn special_folder_alias(bytes: &[u8], expected_folder_id: u64) -> Option<MapiSpecialFolderAlias>`

# Called by

- [default_folder_entry_id_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_aliases.md)
- [indexed_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/indexed_special_folder_aliases.md)