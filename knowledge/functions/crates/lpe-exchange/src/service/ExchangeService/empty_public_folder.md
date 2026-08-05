---
type: Rust Method
title: empty_public_folder
resource: crates/lpe-exchange/src/service.rs#L956-L1013
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder
---

# Signature

`async fn empty_public_folder( &self, principal: &AccountPrincipal, folder_id: Uuid, delete_subfolders: bool, ) -> Result<()>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [empty_folder](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder.md)