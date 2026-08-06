---
type: Rust Function
title: parse_attachment_reference
resource: crates/lpe-exchange/src/tests/mod.rs#L12738-L12746
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_message_attachment
---

# Signature

`fn parse_attachment_reference(value: &str) -> Option<(Uuid, Uuid)>`

# Called by

- [delete_message_attachment](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/delete_message_attachment.md)