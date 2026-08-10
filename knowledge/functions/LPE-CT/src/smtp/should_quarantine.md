---
type: Rust Function
title: should_quarantine
resource: LPE-CT/src/smtp.rs#L1231-L1236
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
---

# Signature

`fn should_quarantine(data: &[u8]) -> bool`

# Called by

- [process_outbound_handoff](../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [evaluate_inbound_policy](../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)