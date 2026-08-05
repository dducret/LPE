---
type: Rust Function
title: parse_antivirus_output
resource: LPE-CT/src/smtp/antivirus.rs#L421-L462
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/antivirus/marker_matches
  - functions/LPE-CT/src/smtp/antivirus/takeri_summary_count
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/run_antivirus_provider
  - functions/LPE-CT/src/smtp/tests/antivirus_output_parser_detects_takeri_infections_and_suspicious_files
  - functions/LPE-CT/src/smtp/tests/antivirus_output_parser_ignores_negative_takeri_markers
---

# Signature

`pub(crate) fn parse_antivirus_output( provider: &AntivirusProviderConfig, stdout: &str, stderr: &str, exit_code: Option<i32>, ) -> Result<AntivirusScanOutcome>`

# Calls

- [marker_matches](../../../../../functions/LPE-CT/src/smtp/antivirus/marker_matches.md)
- [takeri_summary_count](../../../../../functions/LPE-CT/src/smtp/antivirus/takeri_summary_count.md)

# Called by

- [run_antivirus_provider](../../../../../functions/LPE-CT/src/smtp/antivirus/run_antivirus_provider.md)
- [antivirus_output_parser_detects_takeri_infections_and_suspicious_files](../../../../../functions/LPE-CT/src/smtp/tests/antivirus_output_parser_detects_takeri_infections_and_suspicious_files.md)
- [antivirus_output_parser_ignores_negative_takeri_markers](../../../../../functions/LPE-CT/src/smtp/tests/antivirus_output_parser_ignores_negative_takeri_markers.md)