---
type: Rust Function
title: non_empty_env
resource: LPE-CT/src/imaps_proxy.rs#L131-L136
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/imaps_proxy/imaps_bind_address
  - functions/LPE-CT/src/imaps_proxy/imaps_upstream_address
  - functions/LPE-CT/src/imaps_proxy/imaps_tls_cert_path
  - functions/LPE-CT/src/imaps_proxy/imaps_tls_key_path
---

# Signature

`fn non_empty_env(name: &str) -> Option<String>`

# Called by

- [imaps_bind_address](../../../../functions/LPE-CT/src/imaps_proxy/imaps_bind_address.md)
- [imaps_upstream_address](../../../../functions/LPE-CT/src/imaps_proxy/imaps_upstream_address.md)
- [imaps_tls_cert_path](../../../../functions/LPE-CT/src/imaps_proxy/imaps_tls_cert_path.md)
- [imaps_tls_key_path](../../../../functions/LPE-CT/src/imaps_proxy/imaps_tls_key_path.md)