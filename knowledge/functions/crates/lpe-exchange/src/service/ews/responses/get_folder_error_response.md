---
type: Rust Function
title: get_folder_error_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L37-L54
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder
---

# Signature

`pub(in crate::service) fn get_folder_error_response(code: &str, message: &str) -> String`

# Called by

- [get_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder.md)