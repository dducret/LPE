---
type: Rust Method
title: copy_public_folder_tree
resource: crates/lpe-exchange/src/service.rs#L886-L954
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder
---

# Signature

`async fn copy_public_folder_tree( &self, principal: &AccountPrincipal, source_id: Uuid, target_parent_id: Uuid, ) -> Result<PublicFolder>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [copy_folder](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder.md)