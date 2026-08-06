---
type: Rust Method
title: collection_folder_xml
resource: crates/lpe-exchange/src/service/ews/folders.rs#L466-L482
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/folders/folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/collection_folder_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/find_folder
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/sync_folder_hierarchy
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder
---

# Signature

`async fn collection_folder_xml( &self, collection: &CollaborationCollection, distinguished_id: &str, class: &str, ) -> Result<String>`

# Calls

- [folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/folder_xml.md)
- [collection_folder_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/collection_folder_change_key.md)

# Called by

- [find_folder](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/find_folder.md)
- [sync_folder_hierarchy](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/sync_folder_hierarchy.md)
- [get_folder](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/get_folder.md)