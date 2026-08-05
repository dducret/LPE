---
type: Rust Function
title: requested_sharing_kind
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L298-L313
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_metadata
  - functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request
---

# Signature

`pub(in crate::service) fn requested_sharing_kind( request: &str, ) -> Option<CollaborationResourceKind>`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [get_sharing_metadata](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_metadata.md)
- [parse_sharing_request](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request.md)