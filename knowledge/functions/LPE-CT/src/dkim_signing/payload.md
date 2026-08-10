---
type: Rust Function
title: payload
resource: LPE-CT/src/dkim_signing.rs#L179-L203
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dkim_signing/dkim_signer_adds_header_when_domain_key_exists
  - functions/LPE-CT/src/dkim_signing/dkim_signer_prefers_from_domain_before_sender_domain
  - functions/LPE-CT/web/app/openPlatformDrawer
  - functions/crates/lpe-storage/src/change/CanonicalChangeListener/wait_for_change
---

# Signature

`fn payload() -> OutboundMessageHandoffRequest`

# Called by

- [dkim_signer_adds_header_when_domain_key_exists](../../../../functions/LPE-CT/src/dkim_signing/dkim_signer_adds_header_when_domain_key_exists.md)
- [dkim_signer_prefers_from_domain_before_sender_domain](../../../../functions/LPE-CT/src/dkim_signing/dkim_signer_prefers_from_domain_before_sender_domain.md)
- [openPlatformDrawer](../../../../functions/LPE-CT/web/app/openPlatformDrawer.md)
- [wait_for_change](../../../../functions/crates/lpe-storage/src/change/CanonicalChangeListener/wait_for_change.md)