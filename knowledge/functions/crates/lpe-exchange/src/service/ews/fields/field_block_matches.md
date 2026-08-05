---
type: Rust Function
title: field_block_matches
resource: crates/lpe-exchange/src/service/ews/fields.rs#L37-L52
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/fields/field_deleted
---

# Signature

`fn field_block_matches(block: &str, field_uri: &str) -> bool`

# Calls

- [attribute_values_for_tag](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)

# Called by

- [field_deleted](../../../../../../../functions/crates/lpe-exchange/src/service/ews/fields/field_deleted.md)