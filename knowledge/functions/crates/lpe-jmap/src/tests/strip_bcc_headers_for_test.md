---
type: Rust Function
title: strip_bcc_headers_for_test
resource: crates/lpe-jmap/src/tests.rs#L180-L204
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/fetch_jmap_message_blob
---

# Signature

`fn strip_bcc_headers_for_test(raw: &[u8]) -> Vec<u8>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [fetch_jmap_message_blob](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/jmapstore/fetch_jmap_message_blob.md)