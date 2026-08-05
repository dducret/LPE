---
type: Rust Function
title: marker_matches
resource: LPE-CT/src/smtp/antivirus.rs#L464-L470
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/antivirus/marker_has_positive_match
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/parse_antivirus_output
---

# Signature

`fn marker_matches(output: &str, markers: &[String]) -> bool`

# Calls

- [marker_has_positive_match](../../../../../functions/LPE-CT/src/smtp/antivirus/marker_has_positive_match.md)

# Called by

- [parse_antivirus_output](../../../../../functions/LPE-CT/src/smtp/antivirus/parse_antivirus_output.md)