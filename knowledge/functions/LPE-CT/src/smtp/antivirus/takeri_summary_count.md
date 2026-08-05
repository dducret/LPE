---
type: Rust Function
title: takeri_summary_count
resource: LPE-CT/src/smtp/antivirus.rs#L513-L524
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/parse_antivirus_output
---

# Signature

`fn takeri_summary_count(output: &str, prefix: &str) -> usize`

# Called by

- [parse_antivirus_output](../../../../../functions/LPE-CT/src/smtp/antivirus/parse_antivirus_output.md)