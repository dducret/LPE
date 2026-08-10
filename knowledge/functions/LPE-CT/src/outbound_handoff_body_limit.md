---
type: Rust Function
title: outbound_handoff_body_limit
resource: LPE-CT/src/main.rs#L834-L840
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/protocol/max_smtp_message_size_bytes
  called_by:
  - functions/LPE-CT/src/router
---

# Signature

`fn outbound_handoff_body_limit(max_message_size_mb: u32) -> usize`

# Calls

- [max_smtp_message_size_bytes](../../../functions/LPE-CT/src/smtp/protocol/max_smtp_message_size_bytes.md)

# Called by

- [router](../../../functions/LPE-CT/src/router.md)