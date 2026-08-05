---
type: Rust Function
title: normalize_public_tls_settings
resource: LPE-CT/src/dashboard_config.rs#L243-L264
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dashboard_config/apply_env_overrides
  - functions/LPE-CT/src/http_routes/update_network
  - functions/LPE-CT/src/http_routes/upload_public_tls_profile
  - functions/LPE-CT/src/http_routes/select_public_tls_profile
  - functions/LPE-CT/src/http_routes/delete_public_tls_profile
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) fn normalize_public_tls_settings(settings: &mut PublicTlsSettings)`

# Called by

- [apply_env_overrides](../../../../functions/LPE-CT/src/dashboard_config/apply_env_overrides.md)
- [update_network](../../../../functions/LPE-CT/src/http_routes/update_network.md)
- [upload_public_tls_profile](../../../../functions/LPE-CT/src/http_routes/upload_public_tls_profile.md)
- [select_public_tls_profile](../../../../functions/LPE-CT/src/http_routes/select_public_tls_profile.md)
- [delete_public_tls_profile](../../../../functions/LPE-CT/src/http_routes/delete_public_tls_profile.md)
- [main](../../../../functions/LPE-CT/src/main.md)