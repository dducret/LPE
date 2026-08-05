---
type: Rust Method
title: fetch_ews_searchable_mailboxes
resource: crates/lpe-exchange/src/tests/mod.rs#L5113-L5138
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_searchable_mailboxes
---

# Signature

`fn fetch_ews_searchable_mailboxes<'a>( &'a self, principal: &'a AccountPrincipal, ) -> StoreFuture<'a, Vec<EwsSearchableMailbox>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [get_searchable_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/compliance/ExchangeService/get_searchable_mailboxes.md)