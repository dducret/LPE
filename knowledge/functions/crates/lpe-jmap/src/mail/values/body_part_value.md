---
type: Rust Function
title: body_part_value
resource: crates/lpe-jmap/src/mail/values.rs#L453-L465
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/insert_if
---

# Signature

`fn body_part_value( part_id: &str, content_type: &str, size: usize, properties: &HashSet<String>, ) -> Value`

# Calls

- [insert_if](../../../../../../functions/crates/lpe-jmap/src/convert/insert_if.md)