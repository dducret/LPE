---
type: Rust Function
title: outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay
resource: LPE-CT/src/smtp/tests.rs#L1844-L1879
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/tests/outbound_request
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`async fn outbound_handoff_replay_after_relay_reuses_sent_custody_without_second_relay()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_smtp_with_profile](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)