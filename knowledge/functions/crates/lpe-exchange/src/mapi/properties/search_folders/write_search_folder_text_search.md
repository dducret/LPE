---
type: Rust Function
title: write_search_folder_text_search
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L281-L294
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob
---

# Signature

`fn write_search_folder_text_search(blob: &mut Vec<u8>, text: Option<&str>)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [search_folder_definition_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob.md)