---
type: Rust Function
title: folder_xml
resource: crates/lpe-exchange/src/service/ews/folders.rs#L737-L775
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/find_folder
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/sync_folder_hierarchy
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder
---

# Signature

`pub(in crate::service) fn folder_xml( collection: &CollaborationCollection, distinguished_id: &str, class: &str, ) -> String`

# Called by

- [find_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/find_folder.md)
- [sync_folder_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/sync_folder_hierarchy.md)
- [get_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder.md)