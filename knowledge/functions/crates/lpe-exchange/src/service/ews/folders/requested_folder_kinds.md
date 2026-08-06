---
type: Rust Function
title: requested_folder_kinds
resource: crates/lpe-exchange/src/service/ews/folders.rs#L563-L611
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder
---

# Signature

`pub(in crate::service) fn requested_folder_kinds(request: &str) -> Vec<FolderKind>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [requested_mailbox_role](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role.md)

# Called by

- [get_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder.md)