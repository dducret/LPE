---
type: Rust Function
title: marker_has_positive_match
resource: LPE-CT/src/smtp/antivirus.rs#L472-L483
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/antivirus/marker_match_is_explicitly_negative
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/marker_matches
---

# Signature

`fn marker_has_positive_match(output: &str, marker: &str) -> bool`

# Calls

- [marker_match_is_explicitly_negative](../../../../../functions/LPE-CT/src/smtp/antivirus/marker_match_is_explicitly_negative.md)

# Called by

- [marker_matches](../../../../../functions/LPE-CT/src/smtp/antivirus/marker_matches.md)