---
type: Rust Function
title: outbound_handoff_relays_message
resource: LPE-CT/src/smtp/tests.rs#L1762-L1841
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/tests/runtime_config
---

# Signature

`async fn outbound_handoff_relays_message()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_smtp_with_profile](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)