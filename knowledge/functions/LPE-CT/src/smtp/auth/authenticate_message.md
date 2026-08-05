---
type: Rust Function
title: authenticate_message
resource: LPE-CT/src/smtp/auth.rs#L220-L284
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/auth/summarize_spf
  - functions/LPE-CT/src/smtp/auth/summarize_dkim
  - functions/LPE-CT/src/smtp/auth/summarize_dmarc
  - functions/LPE-CT/src/smtp/auth/spf_disposition
  - functions/LPE-CT/src/smtp/auth/dkim_disposition
  - functions/LPE-CT/src/smtp/auth/AuthenticationAssessment/has_temporary_failure
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
---

# Signature

`pub(in crate::smtp) async fn authenticate_message( client_ip: IpAddr, helo: &str, mail_from: &str, message_bytes: &[u8], ) -> Result<( AuthSummary, Vec<DecisionTraceEntry>, AuthenticationAssessment, )>`

# Calls

- [summarize_spf](../../../../../functions/LPE-CT/src/smtp/auth/summarize_spf.md)
- [summarize_dkim](../../../../../functions/LPE-CT/src/smtp/auth/summarize_dkim.md)
- [summarize_dmarc](../../../../../functions/LPE-CT/src/smtp/auth/summarize_dmarc.md)
- [spf_disposition](../../../../../functions/LPE-CT/src/smtp/auth/spf_disposition.md)
- [dkim_disposition](../../../../../functions/LPE-CT/src/smtp/auth/dkim_disposition.md)
- [has_temporary_failure](../../../../../functions/LPE-CT/src/smtp/auth/AuthenticationAssessment/has_temporary_failure.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)