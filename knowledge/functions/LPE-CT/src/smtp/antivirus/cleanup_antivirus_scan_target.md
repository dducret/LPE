---
type: Rust Function
title: cleanup_antivirus_scan_target
resource: LPE-CT/src/smtp/antivirus.rs#L390-L392
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy
---

# Signature

`fn cleanup_antivirus_scan_target(target: &AntivirusScanTarget)`

# Called by

- [evaluate_antivirus_policy](../../../../../functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy.md)