---
type: Rust Function
title: ensure_not_replayed
resource: LPE-CT/src/management_auth.rs#L118-L134
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/replay_key
---

# Signature

`fn ensure_not_replayed(signed: &SignedIntegrationHeaders) -> Result<(), BridgeAuthError>`

# Calls

- [replay_key](../../../../functions/crates/lpe-domain/src/bridge_auth/SignedIntegrationHeaders/replay_key.md)