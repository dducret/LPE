---
type: Rust Function
title: run_antivirus_provider
resource: LPE-CT/src/smtp/antivirus.rs#L394-L419
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/antivirus/parse_antivirus_output
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy
---

# Signature

`async fn run_antivirus_provider( provider: &AntivirusProviderConfig, target: &AntivirusScanTarget, ) -> Result<AntivirusScanOutcome>`

# Calls

- [parse_antivirus_output](../../../../../functions/LPE-CT/src/smtp/antivirus/parse_antivirus_output.md)

# Called by

- [evaluate_antivirus_policy](../../../../../functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy.md)