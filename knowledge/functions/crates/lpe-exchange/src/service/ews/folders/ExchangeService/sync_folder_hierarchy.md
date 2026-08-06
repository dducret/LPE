---
type: Rust Method
title: sync_folder_hierarchy
resource: crates/lpe-exchange/src/service/ews/folders.rs#L83-L199
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state
  - functions/crates/lpe-exchange/src/service/ews/folders/hierarchy_sync_state_items
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/ews/folders/hierarchy_sync_state
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn sync_folder_hierarchy( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [mailbox_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml.md)
- [folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/folder_xml.md)
- [public_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml.md)
- [requested_sync_state](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state.md)
- [hierarchy_sync_state_items](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/hierarchy_sync_state_items.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [hierarchy_sync_state](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/hierarchy_sync_state.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)