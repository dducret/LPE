---
type: Rust Function
title: benchmark_relay_hot_path
resource: LPE-CT/src/smtp/tests.rs#L3626-L3654
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

`async fn benchmark_relay_hot_path()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [spawn_dummy_smtp](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)