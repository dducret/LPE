---
type: Rust Function
title: decode_pst_field
resource: crates/lpe-storage/src/pst.rs#L577-L599
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst
---

# Signature

`fn decode_pst_field(value: &str) -> String`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [import_mailbox_from_pst](../../../../../functions/crates/lpe-storage/src/pst/Storage/import_mailbox_from_pst.md)