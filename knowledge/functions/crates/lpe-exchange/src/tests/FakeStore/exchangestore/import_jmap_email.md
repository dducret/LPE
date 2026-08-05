---
type: Rust Method
title: import_jmap_email
resource: crates/lpe-exchange/src/tests/mod.rs#L11244-L11292
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/FakeStore/email_addresses
---

# Signature

`fn import_jmap_email<'a>( &'a self, input: JmapImportedEmailInput, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, JmapEmail>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [email_addresses](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/email_addresses.md)