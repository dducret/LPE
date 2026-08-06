---
type: Rust Function
title: validate_supplied_folder_change_key
resource: crates/lpe-exchange/src/service/ews/folders.rs#L674-L683
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
---

# Signature

`pub(in crate::service) fn validate_supplied_folder_change_key( supplied_change_key: Option<&str>, current_change_key: &str, id: &str, ) -> Result<()>`

# Called by

- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)