---
type: Rust Method
title: validate_bytes
resource: crates/lpe-domain/src/bridge_auth.rs#L111-L146
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/bridge_auth/sign_components
---

# Signature

`pub fn validate_bytes( &self, shared_secret: &str, method: &str, path: &str, payload: &[u8], now: i64, max_skew_seconds: i64, ) -> Result<(), BridgeAuthError>`

# Calls

- [sign_components](../../../../../../functions/crates/lpe-domain/src/bridge_auth/sign_components.md)