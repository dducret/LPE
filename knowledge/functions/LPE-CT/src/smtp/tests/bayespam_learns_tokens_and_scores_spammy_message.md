---
type: Rust Function
title: bayespam_learns_tokens_and_scores_spammy_message
resource: LPE-CT/src/smtp/tests.rs#L2555-L2596
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/smtp/tests/training_message
  - functions/LPE-CT/src/smtp/bayes/load_bayespam_corpus
  - functions/LPE-CT/src/smtp/bayes/score_bayespam
---

# Signature

`async fn bayespam_learns_tokens_and_scores_spammy_message()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [training_message](../../../../../functions/LPE-CT/src/smtp/tests/training_message.md)
- [load_bayespam_corpus](../../../../../functions/LPE-CT/src/smtp/bayes/load_bayespam_corpus.md)
- [score_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam.md)