---
type: Rust Method
title: replay_key
resource: crates/lpe-domain/src/bridge_auth.rs#L148-L155
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/management_auth/ensure_not_replayed
  - functions/crates/lpe-admin-api/src/integration/ensure_not_replayed
---

# Signature

`pub fn replay_key(&self) -> String`

# Called by

- [ensure_not_replayed](../../../../../../functions/LPE-CT/src/management_auth/ensure_not_replayed.md)
- [ensure_not_replayed](../../../../../../functions/crates/lpe-admin-api/src/integration/ensure_not_replayed.md)