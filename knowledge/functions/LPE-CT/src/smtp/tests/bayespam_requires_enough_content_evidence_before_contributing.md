---
type: Rust Function
title: bayespam_requires_enough_content_evidence_before_contributing
resource: LPE-CT/src/smtp/tests.rs#L2599-L2635
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/smtp/tests/training_message
  - functions/LPE-CT/src/smtp/bayes/score_bayespam
---

# Signature

`async fn bayespam_requires_enough_content_evidence_before_contributing()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [training_message](../../../../../functions/LPE-CT/src/smtp/tests/training_message.md)
- [score_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam.md)