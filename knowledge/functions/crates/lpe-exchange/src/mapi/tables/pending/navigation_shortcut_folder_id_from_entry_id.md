---
type: Rust Function
title: navigation_shortcut_folder_id_from_entry_id
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L134-L140
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties
---

# Signature

`fn navigation_shortcut_folder_id_from_entry_id(bytes: &[u8]) -> Option<u64>`

# Called by

- [navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties.md)