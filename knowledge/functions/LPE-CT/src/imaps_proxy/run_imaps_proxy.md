---
type: Rust Function
title: run_imaps_proxy
resource: LPE-CT/src/imaps_proxy.rs#L13-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/imaps_proxy/handle_imaps_session
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) async fn run_imaps_proxy( bind_address: String, upstream_address: String, cert_path: PathBuf, key_path: PathBuf, ) -> Result<()>`

# Calls

- [handle_imaps_session](../../../../functions/LPE-CT/src/imaps_proxy/handle_imaps_session.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)