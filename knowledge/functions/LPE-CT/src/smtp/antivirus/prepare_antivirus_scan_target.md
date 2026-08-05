---
type: Rust Function
title: prepare_antivirus_scan_target
resource: LPE-CT/src/smtp/antivirus.rs#L329-L375
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts
  called_by:
  - functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy
---

# Signature

`fn prepare_antivirus_scan_target( direction: &str, message_bytes: &[u8], ) -> Result<AntivirusScanTarget>`

# Calls

- [collect_mime_attachment_parts](../../../../../functions/crates/lpe-magika/src/mime/collect_mime_attachment_parts.md)

# Called by

- [evaluate_antivirus_policy](../../../../../functions/LPE-CT/src/smtp/antivirus/evaluate_antivirus_policy.md)