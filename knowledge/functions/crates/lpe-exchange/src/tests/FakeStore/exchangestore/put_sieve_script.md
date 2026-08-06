---
type: Rust Method
title: put_sieve_script
resource: crates/lpe-exchange/src/tests/mod.rs#L9465-L9512
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn put_sieve_script<'a>( &'a self, _account_id: Uuid, name: &'a str, content: &'a str, activate: bool, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, SieveScriptDocument>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)