---
type: Rust Function
title: folders_operation_success_response
resource: crates/lpe-exchange/src/service/ews/folders.rs#L590-L608
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/move_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder
---

# Signature

`pub(in crate::service) fn folders_operation_success_response( operation: &str, folders: String, ) -> String`

# Called by

- [create_folder_path](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path.md)
- [copy_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder.md)
- [move_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/move_folder.md)
- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)
- [create_managed_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/create_managed_folder.md)