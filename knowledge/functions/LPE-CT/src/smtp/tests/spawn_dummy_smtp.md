---
type: Rust Function
title: spawn_dummy_smtp
resource: LPE-CT/src/smtp/tests.rs#L3357-L3363
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile
  called_by:
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule
  - functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path
---

# Signature

`async fn spawn_dummy_smtp(captured: Arc<Mutex<String>>) -> String`

# Calls

- [spawn_dummy_smtp_with_profile](../../../../../functions/LPE-CT/src/smtp/tests/spawn_dummy_smtp_with_profile.md)

# Called by

- [outbound_handoff_defers_when_local_throttle_hits](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_defers_when_local_throttle_hits.md)
- [outbound_handoff_uses_matching_routing_rule](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_uses_matching_routing_rule.md)
- [benchmark_relay_hot_path](../../../../../functions/LPE-CT/src/smtp/tests/benchmark_relay_hot_path.md)