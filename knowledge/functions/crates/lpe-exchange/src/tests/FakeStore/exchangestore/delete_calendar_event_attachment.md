---
type: Rust Method
title: delete_calendar_event_attachment
resource: crates/lpe-exchange/src/tests/mod.rs#L11148-L11169
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/attachments/parse_calendar_attachment_file_reference
---

# Signature

`fn delete_calendar_event_attachment<'a>( &'a self, _account_id: Uuid, file_reference: &'a str, _audit: lpe_storage::AuditEntryInput, ) -> StoreFuture<'a, Option<Uuid>>`

# Calls

- [parse_calendar_attachment_file_reference](../../../../../../../functions/crates/lpe-storage/src/attachments/parse_calendar_attachment_file_reference.md)