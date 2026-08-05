---
type: Rust Function
title: score_bayespam
resource: LPE-CT/src/smtp/bayes.rs#L158-L181
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/bayes/load_bayespam_corpus
  - functions/LPE-CT/src/smtp/bayes/tokenize_for_bayespam
  - functions/LPE-CT/src/smtp/bayes/score_bayespam_tokens
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message
  - functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing
---

# Signature

`pub(crate) async fn score_bayespam( spool_dir: &Path, config: &RuntimeConfig, subject: &str, visible_text: &str, _mail_from: &str, _helo: &str, ) -> Result<Option<BayesOutcome>>`

# Calls

- [load_bayespam_corpus](../../../../../functions/LPE-CT/src/smtp/bayes/load_bayespam_corpus.md)
- [tokenize_for_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/tokenize_for_bayespam.md)
- [score_bayespam_tokens](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam_tokens.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [bayespam_learns_tokens_and_scores_spammy_message](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message.md)
- [bayespam_requires_enough_content_evidence_before_contributing](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing.md)