---
type: Rust Function
title: get_sharing_metadata_response
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L190-L213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/sharing/sharing_metadata_entry_xml
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_metadata
---

# Signature

`pub(in crate::service) fn get_sharing_metadata_response( principal: &AccountPrincipal, contact_collections: &[CollaborationCollection], calendar_collections: &[CollaborationCollection], ) -> String`

# Calls

- [sharing_metadata_entry_xml](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/sharing_metadata_entry_xml.md)

# Called by

- [get_sharing_metadata](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_metadata.md)