---
type: Rust Function
title: folder_change_key
resource: crates/lpe-exchange/src/service/ews/folders.rs#L925-L927
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml
---

# Signature

`pub(in crate::service) fn folder_change_key(id: &str) -> String`

# Called by

- [mailbox_folder_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml.md)
- [public_folder_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml.md)