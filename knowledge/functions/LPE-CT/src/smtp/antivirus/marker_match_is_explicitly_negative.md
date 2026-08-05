---
type: Rust Function
title: marker_match_is_explicitly_negative
resource: LPE-CT/src/smtp/antivirus.rs#L485-L511
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/marker_has_positive_match
---

# Signature

`fn marker_match_is_explicitly_negative( output: &str, marker_start: usize, marker_end: usize, ) -> bool`

# Called by

- [marker_has_positive_match](../../../../../functions/LPE-CT/src/smtp/antivirus/marker_has_positive_match.md)