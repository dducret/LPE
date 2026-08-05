---
type: Rust Function
title: load_bayespam_corpus
resource: LPE-CT/src/smtp/bayes.rs#L26-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/LPE-CT/src/smtp/bayes/score_bayespam
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message
---

# Signature

`pub(crate) async fn load_bayespam_corpus( spool_dir: &Path, config: &RuntimeConfig, ) -> Result<BayesCorpus>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [score_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [bayespam_learns_tokens_and_scores_spammy_message](../../../../../functions/LPE-CT/src/smtp/tests/bayespam_learns_tokens_and_scores_spammy_message.md)