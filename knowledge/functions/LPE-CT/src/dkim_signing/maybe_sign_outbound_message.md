---
type: Rust Function
title: maybe_sign_outbound_message
resource: LPE-CT/src/dkim_signing.rs#L29-L85
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dkim_signing/select_signing_key
  - functions/LPE-CT/src/dkim_signing/split_body
  called_by:
  - functions/LPE-CT/src/dkim_signing/dkim_signer_adds_header_when_domain_key_exists
  - functions/LPE-CT/src/dkim_signing/dkim_signer_prefers_from_domain_before_sender_domain
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`pub(crate) fn maybe_sign_outbound_message( config: &DkimConfig, payload: &OutboundMessageHandoffRequest, raw_message: &[u8], ) -> Result<DkimSigningOutcome>`

# Calls

- [select_signing_key](../../../../functions/LPE-CT/src/dkim_signing/select_signing_key.md)
- [split_body](../../../../functions/LPE-CT/src/dkim_signing/split_body.md)

# Called by

- [dkim_signer_adds_header_when_domain_key_exists](../../../../functions/LPE-CT/src/dkim_signing/dkim_signer_adds_header_when_domain_key_exists.md)
- [dkim_signer_prefers_from_domain_before_sender_domain](../../../../functions/LPE-CT/src/dkim_signing/dkim_signer_prefers_from_domain_before_sender_domain.md)
- [process_outbound_handoff](../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)