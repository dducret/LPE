---
type: Rust Function
title: sharing_rights
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L339-L354
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request
---

# Signature

`fn sharing_rights(request: &str) -> CollaborationRights`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [parse_sharing_request](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/parse_sharing_request.md)