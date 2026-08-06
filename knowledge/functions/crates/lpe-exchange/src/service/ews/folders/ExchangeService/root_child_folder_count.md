---
type: Rust Method
title: root_child_folder_count
resource: crates/lpe-exchange/src/service/ews/folders.rs#L433-L464
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder
---

# Signature

`pub(in crate::service) async fn root_child_folder_count( &self, principal: &AccountPrincipal, ) -> Result<usize>`

# Called by

- [get_folder](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder.md)