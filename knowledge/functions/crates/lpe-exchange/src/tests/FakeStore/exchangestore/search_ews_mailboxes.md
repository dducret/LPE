---
type: Rust Method
title: search_ews_mailboxes
resource: crates/lpe-exchange/src/tests/mod.rs#L5274-L5346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/search_mailboxes
---

# Signature

`fn search_ews_mailboxes<'a>( &'a self, principal: &'a AccountPrincipal, query_text: &'a str, mailbox_emails: &'a [String], limit: usize, ) -> StoreFuture<'a, EwsDiscoverySearchResult>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [search_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/search_mailboxes.md)