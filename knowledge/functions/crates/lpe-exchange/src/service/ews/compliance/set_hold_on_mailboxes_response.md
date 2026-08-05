---
type: Rust Function
title: set_hold_on_mailboxes_response
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L256-L276
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes
---

# Signature

`pub(in crate::service) fn set_hold_on_mailboxes_response( holds: &[EwsHoldMailbox], enabled: bool, ) -> String`

# Called by

- [set_hold_on_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes.md)