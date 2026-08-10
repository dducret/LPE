---
type: Rust Function
title: outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody
resource: LPE-CT/src/smtp/tests.rs#L2152-L2180
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

`async fn outbound_handoff_replay_for_deferred_message_does_not_duplicate_custody()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_smtp_with_profile](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)