---
type: Rust Function
title: mailbox_folder_change_key
resource: crates/lpe-exchange/src/service/ews/folders.rs#L929-L931
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
---

# Signature

`pub(in crate::service) fn mailbox_folder_change_key(mailbox: &JmapMailbox) -> String`

# Calls

- [versioned_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)

# Called by

- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)