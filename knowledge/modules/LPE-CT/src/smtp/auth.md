---
type: Rust Module
title: auth
resource: LPE-CT/src/smtp/auth.rs#L1-L284
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/email-auth-dkim-dkimresult-dmarc-disposition-as-dmarcdisposition-spf-spfresult-emailauthenticator
  - external/serde-deserialize-serialize
  - external/std-net-ipaddr
  - external/super-decisiontraceentry-systemdnsresolver
  member_of:
  - packages/LPE-CT
---

# Contains

- [AuthSummary](../../../../classes/LPE-CT/src/smtp/auth/AuthSummary.md)
- [AuthenticationAssessment](../../../../classes/LPE-CT/src/smtp/auth/AuthenticationAssessment.md)
- [SpfDisposition](../../../../classes/LPE-CT/src/smtp/auth/SpfDisposition.md)
- [DkimDisposition](../../../../classes/LPE-CT/src/smtp/auth/DkimDisposition.md)
- [has_temporary_failure](../../../../functions/LPE-CT/src/smtp/auth/AuthenticationAssessment/has_temporary_failure.md)
- [apply_authentication_scores](../../../../functions/LPE-CT/src/smtp/auth/apply_authentication_scores.md)
- [spf_disposition](../../../../functions/LPE-CT/src/smtp/auth/spf_disposition.md)
- [dkim_disposition](../../../../functions/LPE-CT/src/smtp/auth/dkim_disposition.md)
- [summarize_spf](../../../../functions/LPE-CT/src/smtp/auth/summarize_spf.md)
- [summarize_dkim](../../../../functions/LPE-CT/src/smtp/auth/summarize_dkim.md)
- [summarize_dmarc](../../../../functions/LPE-CT/src/smtp/auth/summarize_dmarc.md)
- [authenticate_message](../../../../functions/LPE-CT/src/smtp/auth/authenticate_message.md)

# Imports

- `anyhow::{anyhow, Result}`
- `email_auth::{
    dkim::DkimResult, dmarc::Disposition as DmarcDisposition, spf::SpfResult, EmailAuthenticator,
}`
- `serde::{Deserialize, Serialize}`
- `std::net::IpAddr`
- `super::{DecisionTraceEntry, SystemDnsResolver}`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)