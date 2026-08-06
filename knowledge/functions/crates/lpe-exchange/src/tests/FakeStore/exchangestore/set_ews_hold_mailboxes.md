---
type: Rust Method
title: set_ews_hold_mailboxes
resource: crates/lpe-exchange/src/tests/mod.rs#L5451-L5528
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes
---

# Signature

`fn set_ews_hold_mailboxes<'a>( &'a self, principal: &'a AccountPrincipal, hold_name: &'a str, query_text: &'a str, mailbox_emails: &'a [String], enable: bool, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, Vec<EwsHoldMailbox>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [set_hold_on_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/set_hold_on_mailboxes.md)