---
type: Rust Function
title: bayespam_token_probability
resource: LPE-CT/src/smtp/bayes.rs#L102-L115
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/smtp/bayes/score_bayespam_tokens
---

# Signature

`fn bayespam_token_probability(corpus: &BayesCorpus, token: &str) -> Option<f64>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [score_bayespam_tokens](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam_tokens.md)