---
type: Rust Function
title: dkim_key_status
resource: LPE-CT/src/readiness.rs#L388-L402
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/http_routes/policy_status
---

# Signature

`pub(crate) fn dkim_key_status(path: &str) -> String`

# Called by

- [policy_status](../../../../functions/LPE-CT/src/http_routes/policy_status.md)