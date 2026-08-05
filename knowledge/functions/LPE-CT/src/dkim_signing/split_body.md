---
type: Rust Function
title: split_body
resource: LPE-CT/src/dkim_signing.rs#L120-L126
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message
---

# Signature

`fn split_body(message: &[u8]) -> Vec<u8>`

# Calls

- [position](../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [maybe_sign_outbound_message](../../../../functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message.md)