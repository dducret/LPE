---
type: Rust Function
title: folder_xml
resource: crates/lpe-exchange/src/service/ews/folders.rs#L791-L835
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml
---

# Signature

`pub(in crate::service) fn folder_xml( collection: &CollaborationCollection, distinguished_id: &str, class: &str, change_key: &str, ) -> String`

# Called by

- [collection_folder_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml.md)