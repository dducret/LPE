---
type: Rust Function
title: get_hold_on_mailboxes_response
resource: crates/lpe-exchange/src/service/ews/compliance.rs#L239-L254
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_hold_on_mailboxes
---

# Signature

`pub(in crate::service) fn get_hold_on_mailboxes_response(holds: &[EwsHoldMailbox]) -> String`

# Called by

- [get_hold_on_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_hold_on_mailboxes.md)