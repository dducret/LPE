---
type: Rust Function
title: upsert_env_public_tls_profile
resource: LPE-CT/src/dashboard_config.rs#L219-L241
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/dashboard_config/apply_env_overrides
---

# Signature

`fn upsert_env_public_tls_profile( settings: &mut PublicTlsSettings, cert_path: String, key_path: String, )`

# Calls

- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [apply_env_overrides](../../../../functions/LPE-CT/src/dashboard_config/apply_env_overrides.md)