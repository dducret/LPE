---
type: Rust Function
title: training_message
resource: LPE-CT/src/smtp/tests.rs#L1368-L1394
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message
  - functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score
---

# Signature

`fn training_message(subject: &str, body: &str) -> QueuedMessage`

# Called by

- [bayespam_learns_tokens_and_scores_spammy_message](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message.md)
- [bayespam_requires_enough_content_evidence_before_contributing](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing.md)
- [outbound_handoff_quarantines_on_bayespam_score](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score.md)