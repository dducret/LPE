---
type: Rust Module
title: dkim_signing
resource: LPE-CT/src/dkim_signing.rs#L1-L304
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-context-result
  - external/email-auth-dkim-canonicalizationmethod-dkimsigner
  - external/lpe-domain-outboundmessagehandoffrequest
  - external/std-fs
  - external/super-maybe-sign-outbound-message-dkimconfig-dkimkeyconfig
  - external/crate-env-test-lock
  - external/lpe-domain-outboundmessagehandoffrequest-transportrecipient
  - external/uuid-uuid
  member_of:
  - packages/LPE-CT
---

# Contains

- [DkimSigningOutcome](../../../classes/LPE-CT/src/dkim_signing/DkimSigningOutcome.md)
- [DkimKeyConfig](../../../classes/LPE-CT/src/dkim_signing/DkimKeyConfig.md)
- [DkimConfig](../../../classes/LPE-CT/src/dkim_signing/DkimConfig.md)
- [maybe_sign_outbound_message](../../../functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message.md)
- [parse_headers](../../../functions/LPE-CT/src/dkim_signing/parse_headers.md)
- [split_body](../../../functions/LPE-CT/src/dkim_signing/split_body.md)
- [select_signing_key](../../../functions/LPE-CT/src/dkim_signing/select_signing_key.md)
- [signing_domain_candidates](../../../functions/LPE-CT/src/dkim_signing/signing_domain_candidates.md)
- [address_domain](../../../functions/LPE-CT/src/dkim_signing/address_domain.md)
- [payload](../../../functions/LPE-CT/src/dkim_signing/payload.md)
- [dkim_signer_adds_header_when_domain_key_exists](../../../functions/LPE-CT/src/dkim_signing/dkim_signer_adds_header_when_domain_key_exists.md)
- [dkim_signer_prefers_from_domain_before_sender_domain](../../../functions/LPE-CT/src/dkim_signing/dkim_signer_prefers_from_domain_before_sender_domain.md)

# Imports

- `anyhow::{Context, Result}`
- `email_auth::dkim::{CanonicalizationMethod, DkimSigner}`
- `lpe_domain::OutboundMessageHandoffRequest`
- `std::fs`
- `super::{maybe_sign_outbound_message, DkimConfig, DkimKeyConfig}`
- `crate::env_test_lock`
- `lpe_domain::{OutboundMessageHandoffRequest, TransportRecipient}`
- `uuid::Uuid`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)