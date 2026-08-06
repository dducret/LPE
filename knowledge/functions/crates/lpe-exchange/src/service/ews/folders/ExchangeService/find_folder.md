---
type: Rust Method
title: find_folder
resource: crates/lpe-exchange/src/service/ews/folders.rs#L18-L93
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn find_folder( &self, principal: &AccountPrincipal, ) -> Result<String>`

# Calls

- [mailbox_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_xml.md)
- [collection_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ExchangeService/collection_folder_xml.md)
- [public_folder_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_xml.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)