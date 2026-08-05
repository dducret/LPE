---
type: Rust Function
title: evaluate_antivirus_policy
resource: LPE-CT/src/smtp/antivirus.rs#L162-L327
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/src/smtp/antivirus/prepare_antivirus_scan_target
  - functions/LPE-CT/src/smtp/antivirus/run_antivirus_provider
  - functions/LPE-CT/src/smtp/antivirus/cleanup_antivirus_scan_target
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
---

# Signature

`pub(in crate::smtp) async fn evaluate_antivirus_policy( config: &RuntimeConfig, direction: &str, message_bytes: &[u8], ) -> Result<AntivirusVerdict>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [prepare_antivirus_scan_target](../../../../../functions/LPE-CT/src/smtp/antivirus/prepare_antivirus_scan_target.md)
- [run_antivirus_provider](../../../../../functions/LPE-CT/src/smtp/antivirus/run_antivirus_provider.md)
- [cleanup_antivirus_scan_target](../../../../../functions/LPE-CT/src/smtp/antivirus/cleanup_antivirus_scan_target.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)