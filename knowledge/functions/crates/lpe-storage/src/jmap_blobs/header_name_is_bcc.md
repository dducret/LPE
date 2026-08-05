---
type: Rust Function
title: header_name_is_bcc
resource: crates/lpe-storage/src/jmap_blobs.rs#L335-L340
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-storage/src/jmap_blobs/strip_protected_bcc_headers
---

# Signature

`fn header_name_is_bcc(line: &[u8]) -> bool`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [strip_protected_bcc_headers](../../../../../functions/crates/lpe-storage/src/jmap_blobs/strip_protected_bcc_headers.md)