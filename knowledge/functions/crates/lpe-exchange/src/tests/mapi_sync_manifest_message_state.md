---
type: Rust Function
title: mapi_sync_manifest_message_state
resource: crates/lpe-exchange/src/tests/mod.rs#L14727-L14754
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn mapi_sync_manifest_message_state(bytes: &[u8], subject: &str) -> Option<(u32, u32)>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)