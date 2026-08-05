---
type: Rust Function
title: accept_sharing_invitation_response
resource: crates/lpe-exchange/src/service/ews/sharing.rs#L265-L296
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation
---

# Signature

`pub(in crate::service) fn accept_sharing_invitation_response( grant: &CollaborationGrant, change_key: &str, ) -> String`

# Called by

- [accept_sharing_invitation](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation.md)