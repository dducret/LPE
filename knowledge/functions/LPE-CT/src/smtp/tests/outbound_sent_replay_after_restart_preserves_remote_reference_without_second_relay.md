---
type: Rust Function
title: outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay
resource: LPE-CT/src/smtp/tests.rs#L1908-L1952
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile
  - functions/LPE-CT/src/smtp/tests/outbound_request
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/tests/runtime_config
---

# Signature

`async fn outbound_sent_replay_after_restart_preserves_remote_reference_without_second_relay()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_smtp_with_profile](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)