---
type: Rust Function
title: signing_domain_candidates
resource: LPE-CT/src/dkim_signing.rs#L144-L161
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dkim_signing/address_domain
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/dkim_signing/select_signing_key
---

# Signature

`fn signing_domain_candidates(payload: &OutboundMessageHandoffRequest) -> Vec<String>`

# Calls

- [address_domain](../../../../functions/LPE-CT/src/dkim_signing/address_domain.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [select_signing_key](../../../../functions/LPE-CT/src/dkim_signing/select_signing_key.md)