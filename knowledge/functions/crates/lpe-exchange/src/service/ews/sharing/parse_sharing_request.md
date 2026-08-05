---
type: Rust Function
title: parse_sharing_request
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L315-L337
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  - functions/crates/lpe-exchange/src/service/ews/sharing/requested_sharing_kind
  - functions/crates/lpe-exchange/src/service/ews/sharing/sharing_rights
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation
---

# Signature

`pub(in crate::service) fn parse_sharing_request(request: &str) -> Result<SharingRequest>`

# Calls

- [element_content](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [requested_sharing_kind](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/requested_sharing_kind.md)
- [sharing_rights](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/sharing_rights.md)

# Called by

- [get_sharing_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/get_sharing_folder.md)
- [accept_sharing_invitation](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation.md)