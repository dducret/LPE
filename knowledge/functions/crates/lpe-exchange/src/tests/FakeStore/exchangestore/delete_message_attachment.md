---
type: Rust Method
title: delete_message_attachment
resource: crates/lpe-exchange/src/tests/mod.rs#L11118-L11146
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/parse_attachment_reference
---

# Signature

`fn delete_message_attachment<'a>( &'a self, _account_id: Uuid, file_reference: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, Option<JmapEmail>>`

# Calls

- [parse_attachment_reference](../../../../../../../functions/crates/lpe-exchange/src/tests/parse_attachment_reference.md)