---
type: Rust Function
title: tokenize_for_bayespam
resource: LPE-CT/src/smtp/bayes.rs#L76-L100
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/bayes/score_bayespam
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
---

# Signature

`fn tokenize_for_bayespam( subject: &str, visible_text: &str, min_token_length: usize, max_tokens: usize, ) -> Vec<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [score_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/score_bayespam.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)