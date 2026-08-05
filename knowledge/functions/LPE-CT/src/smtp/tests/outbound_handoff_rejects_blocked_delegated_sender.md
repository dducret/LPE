---
type: Rust Function
title: outbound_handoff_rejects_blocked_delegated_sender
resource: LPE-CT/src/smtp/tests.rs#L2676-L2695
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/tests/outbound_request
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`async fn outbound_handoff_rejects_blocked_delegated_sender()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)