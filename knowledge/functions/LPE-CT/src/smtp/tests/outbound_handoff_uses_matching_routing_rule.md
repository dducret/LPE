---
type: Rust Function
title: outbound_handoff_uses_matching_routing_rule
resource: LPE-CT/src/smtp/tests.rs#L2183-L2214
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/tests/outbound_request
---

# Signature

`async fn outbound_handoff_uses_matching_routing_rule()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_smtp](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)