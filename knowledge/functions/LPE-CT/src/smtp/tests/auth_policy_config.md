---
type: Rust Function
title: auth_policy_config
resource: LPE-CT/src/smtp/tests.rs#L3249-L3256
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/tests/runtime_config
  called_by:
  - functions/LPE-CT/src/smtp/tests/strict_dmarc_rejects_spoofed_local_from_without_aligned_auth
  - functions/LPE-CT/src/smtp/tests/external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy
  - functions/LPE-CT/src/smtp/tests/aligned_spf_pass_accepts_message_under_dmarc
  - functions/LPE-CT/src/smtp/tests/aligned_dkim_pass_compensates_for_spf_fail
---

# Signature

`fn auth_policy_config() -> RuntimeConfig`

# Calls

- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)

# Called by

- [strict_dmarc_rejects_spoofed_local_from_without_aligned_auth](../../../../../functions/LPE-CT/src/smtp/tests/strict_dmarc_rejects_spoofed_local_from_without_aligned_auth.md)
- [external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy](../../../../../functions/LPE-CT/src/smtp/tests/external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy.md)
- [aligned_spf_pass_accepts_message_under_dmarc](../../../../../functions/LPE-CT/src/smtp/tests/aligned_spf_pass_accepts_message_under_dmarc.md)
- [aligned_dkim_pass_compensates_for_spf_fail](../../../../../functions/LPE-CT/src/smtp/tests/aligned_dkim_pass_compensates_for_spf_fail.md)