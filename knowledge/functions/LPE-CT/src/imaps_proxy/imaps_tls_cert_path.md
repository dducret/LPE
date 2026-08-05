---
type: Rust Function
title: imaps_tls_cert_path
resource: LPE-CT/src/imaps_proxy.rs#L119-L123
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/imaps_proxy/non_empty_env
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) fn imaps_tls_cert_path() -> Option<PathBuf>`

# Calls

- [non_empty_env](../../../../functions/LPE-CT/src/imaps_proxy/non_empty_env.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)