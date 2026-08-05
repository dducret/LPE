---
type: Rust Function
title: score_bayespam_tokens
resource: LPE-CT/src/smtp/bayes.rs#L117-L156
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/bayes/bayespam_token_probability
  called_by:
  - functions/LPE-CT/src/smtp/bayes/score_bayespam
---

# Signature

`fn score_bayespam_tokens( corpus: &BayesCorpus, tokens: &[String], score_weight: f32, ) -> Option<BayesOutcome>`

# Calls

- [bayespam_token_probability](../../../../../functions/LPE-CT/src/smtp/bayes/bayespam_token_probability.md)

# Called by

- [score_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam.md)