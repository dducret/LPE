---
type: Rust Method
title: fetch_ews_hold_mailboxes
resource: crates/lpe-exchange/src/tests/mod.rs#L5428-L5450
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_hold_on_mailboxes
---

# Signature

`fn fetch_ews_hold_mailboxes<'a>( &'a self, principal: &'a AccountPrincipal, mailbox_emails: &'a [String], ) -> StoreFuture<'a, Vec<EwsHoldMailbox>>`

# Called by

- [get_hold_on_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_hold_on_mailboxes.md)