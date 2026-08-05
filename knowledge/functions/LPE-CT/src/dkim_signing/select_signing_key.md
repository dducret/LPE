---
type: Rust Function
title: select_signing_key
resource: LPE-CT/src/dkim_signing.rs#L128-L142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dkim_signing/signing_domain_candidates
  called_by:
  - functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message
---

# Signature

`fn select_signing_key<'a>( config: &'a DkimConfig, payload: &OutboundMessageHandoffRequest, ) -> Option<&'a DkimKeyConfig>`

# Calls

- [signing_domain_candidates](../../../../functions/LPE-CT/src/dkim_signing/signing_domain_candidates.md)

# Called by

- [maybe_sign_outbound_message](../../../../functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message.md)