---
type: Rust Module
title: antivirus
resource: LPE-CT/src/smtp/antivirus.rs#L1-L524
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/LPE-CT
---

# Contains

- [AntivirusProviderConfig](../../../../classes/LPE-CT/src/smtp/antivirus/AntivirusProviderConfig.md)
- [AntivirusVerdict](../../../../classes/LPE-CT/src/smtp/antivirus/AntivirusVerdict.md)
- [AntivirusProviderDecision](../../../../classes/LPE-CT/src/smtp/antivirus/AntivirusProviderDecision.md)
- [AntivirusScanTarget](../../../../classes/LPE-CT/src/smtp/antivirus/AntivirusScanTarget.md)
- [AntivirusScanOutcome](../../../../classes/LPE-CT/src/smtp/antivirus/AntivirusScanOutcome.md)
- [InboundMagikaOutcome](../../../../classes/LPE-CT/src/smtp/antivirus/InboundMagikaOutcome.md)
- [load_antivirus_providers](../../../../functions/LPE-CT/src/smtp/antivirus/load_antivirus_providers.md)
- [antivirus_provider_from_env](../../../../functions/LPE-CT/src/smtp/antivirus/antivirus_provider_from_env.md)
- [classify_inbound_message](../../../../functions/LPE-CT/src/smtp/antivirus/classify_inbound_message.md)
- [evaluate_antivirus_policy](../../../../functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy.md)
- [prepare_antivirus_scan_target](../../../../functions/LPE-CT/src/smtp/antivirus/prepare_antivirus_scan_target.md)
- [sanitize_attachment_component](../../../../functions/LPE-CT/src/smtp/antivirus/sanitize_attachment_component.md)
- [cleanup_antivirus_scan_target](../../../../functions/LPE-CT/src/smtp/antivirus/cleanup_antivirus_scan_target.md)
- [run_antivirus_provider](../../../../functions/LPE-CT/src/smtp/antivirus/run_antivirus_provider.md)
- [parse_antivirus_output](../../../../functions/LPE-CT/src/smtp/antivirus/parse_antivirus_output.md)
- [marker_matches](../../../../functions/LPE-CT/src/smtp/antivirus/marker_matches.md)
- [marker_has_positive_match](../../../../functions/LPE-CT/src/smtp/antivirus/marker_has_positive_match.md)
- [marker_match_is_explicitly_negative](../../../../functions/LPE-CT/src/smtp/antivirus/marker_match_is_explicitly_negative.md)
- [takeri_summary_count](../../../../functions/LPE-CT/src/smtp/antivirus/takeri_summary_count.md)

# Imports

- `super::*`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)