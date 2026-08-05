---
type: Rust Function
title: verify_recipient_with_core
resource: LPE-CT/src/transport_policy.rs#L123-L286
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/transport_policy/normalize_address
  - functions/LPE-CT/src/transport_policy/cached_recipient_verdict
  - functions/LPE-CT/src/storage/load_recipient_verification_cache_entry
  - functions/LPE-CT/src/transport_policy/recipient_verdict_from_record
  - functions/LPE-CT/src/transport_policy/store_recipient_verdict
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/LPE-CT/src/storage/persist_recipient_verification_cache_entry
  - functions/LPE-CT/src/transport_policy/recipient_verdict_label
  - functions/LPE-CT/src/transport_policy/recipient_verdict_detail
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/transport_policy/recipient_verification_uses_internal_api
---

# Signature

`pub(crate) async fn verify_recipient_with_core( client: &reqwest::Client, config: &RecipientVerificationConfig, core_base_url: &str, sender: Option<&str>, recipient: &str, helo: Option<&str>, peer: Option<&str>, account_id: Option<Uuid>, ) -> RecipientVerificationVerdict`

# Calls

- [normalize_address](../../../../functions/LPE-CT/src/transport_policy/normalize_address.md)
- [cached_recipient_verdict](../../../../functions/LPE-CT/src/transport_policy/cached_recipient_verdict.md)
- [load_recipient_verification_cache_entry](../../../../functions/LPE-CT/src/storage/load_recipient_verification_cache_entry.md)
- [recipient_verdict_from_record](../../../../functions/LPE-CT/src/transport_policy/recipient_verdict_from_record.md)
- [store_recipient_verdict](../../../../functions/LPE-CT/src/transport_policy/store_recipient_verdict.md)
- [sign](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/sign.md)
- [status](../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [persist_recipient_verification_cache_entry](../../../../functions/LPE-CT/src/storage/persist_recipient_verification_cache_entry.md)
- [recipient_verdict_label](../../../../functions/LPE-CT/src/transport_policy/recipient_verdict_label.md)
- [recipient_verdict_detail](../../../../functions/LPE-CT/src/transport_policy/recipient_verdict_detail.md)

# Called by

- [handle_smtp_command](../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [recipient_verification_uses_internal_api](../../../../functions/LPE-CT/src/transport_policy/recipient_verification_uses_internal_api.md)