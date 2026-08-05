---
type: Rust Function
title: push_compressed_rop
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L53-L62
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normalized_rop_sequence_signature
---

# Signature

`fn push_compressed_rop(compressed: &mut Vec<String>, name: &str, count: usize)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [normalized_rop_sequence_signature](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normalized_rop_sequence_signature.md)