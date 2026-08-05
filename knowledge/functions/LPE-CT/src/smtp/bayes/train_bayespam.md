---
type: Rust Function
title: train_bayespam
resource: LPE-CT/src/smtp/bayes.rs#L183-L223
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/crates/lpe-magika/src/mime/extract_visible_text
  - functions/LPE-CT/src/smtp/bayes/tokenize_for_bayespam
  - functions/LPE-CT/src/smtp/bayes/load_bayespam_corpus
  - functions/crates/lpe-jmap/src/state/entry
  - functions/LPE-CT/src/smtp/bayes/save_bayespam_corpus
  called_by:
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message
  - functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing
  - functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score
---

# Signature

`pub(in crate::smtp) async fn train_bayespam( spool_dir: &Path, config: &RuntimeConfig, message: &QueuedMessage, label: BayesLabel, ) -> Result<()>`

# Calls

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [extract_visible_text](../../../../../functions/crates/lpe-magika/src/mime/extract_visible_text.md)
- [tokenize_for_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/tokenize_for_bayespam.md)
- [load_bayespam_corpus](../../../../../functions/LPE-CT/src/smtp/bayes/load_bayespam_corpus.md)
- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [save_bayespam_corpus](../../../../../functions/LPE-CT/src/smtp/bayes/save_bayespam_corpus.md)

# Called by

- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [bayespam_learns_tokens_and_scores_spammy_message](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message.md)
- [bayespam_requires_enough_content_evidence_before_contributing](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_requires_enough_content_evidence_before_contributing.md)
- [outbound_handoff_quarantines_on_bayespam_score](../../../../../functions/LPE-CT/src/smtp/tests/outbound_handoff_quarantines_on_bayespam_score.md)