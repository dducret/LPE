---
type: Rust Function
title: integration_auth_error
resource: crates/lpe-admin-api/src/integration.rs#L583-L593
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/integration/required_header
---

# Signature

`fn integration_auth_error(error: BridgeAuthError) -> (StatusCode, String)`

# Called by

- [required_header](../../../../../functions/crates/lpe-admin-api/src/integration/required_header.md)