---
type: Rust Method
title: create_mapi_event
resource: crates/lpe-exchange/src/tests/mod.rs#L7855-L8064
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference
---

# Signature

`fn create_mapi_event<'a>( &'a self, input: MapiEventCreateInput, ) -> StoreFuture<'a, MapiEventCreateOutcome>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [calendar_attachment_file_reference](../../../../../../../functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference.md)