---
type: Rust Function
title: save_bayespam_corpus
resource: LPE-CT/src/smtp/bayes.rs#L49-L74
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
---

# Signature

`async fn save_bayespam_corpus( spool_dir: &Path, config: &RuntimeConfig, corpus: &BayesCorpus, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)