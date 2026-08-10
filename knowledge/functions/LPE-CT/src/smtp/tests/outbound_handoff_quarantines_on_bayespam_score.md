---
type: Rust Function
title: outbound_handoff_quarantines_on_bayespam_score
resource: LPE-CT/src/smtp/tests.rs#L2614-L2649
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/smtp/tests/training_message
  - functions/LPE-CT/src/smtp/tests/outbound_request
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`async fn outbound_handoff_quarantines_on_bayespam_score()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [training_message](../../../../../functions/LPE-CT/src/smtp/tests/training_message.md)
- [outbound_request](../../../../../functions/LPE-CT/src/smtp/tests/outbound_request.md)
- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)