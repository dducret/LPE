---
type: Rust Function
title: public_folder_change_key
resource: crates/lpe-exchange/src/service/ews/folders.rs#L933-L939
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
---

# Signature

`pub(in crate::service) fn public_folder_change_key(folder: &PublicFolder) -> String`

# Calls

- [versioned_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)

# Called by

- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)