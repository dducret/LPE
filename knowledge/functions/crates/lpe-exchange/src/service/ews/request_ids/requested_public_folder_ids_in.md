---
type: Rust Function
title: requested_public_folder_ids_in
resource: crates/lpe-exchange/src/service/ews/request_ids.rs#L110-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder
---

# Signature

`pub(in crate::service) fn requested_public_folder_ids_in( request: &str, wrapper: &str, ) -> Vec<Uuid>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)

# Called by

- [create_folder_path](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/create_folder_path.md)
- [copy_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder.md)
- [empty_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder.md)