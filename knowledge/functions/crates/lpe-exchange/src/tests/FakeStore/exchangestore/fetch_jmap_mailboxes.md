---
type: Rust Method
title: fetch_jmap_mailboxes
resource: crates/lpe-exchange/src/tests/mod.rs#L9619-L9643
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn fetch_jmap_mailboxes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)