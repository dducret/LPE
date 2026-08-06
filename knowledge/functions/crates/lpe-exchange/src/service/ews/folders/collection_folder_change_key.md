---
type: Rust Function
title: collection_folder_change_key
resource: crates/lpe-exchange/src/service/ews/folders.rs#L941-L943
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml
---

# Signature

`fn collection_folder_change_key(collection: &CollaborationCollection, revision: u64) -> String`

# Calls

- [versioned_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/ids/versioned_change_key.md)

# Called by

- [collection_folder_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml.md)