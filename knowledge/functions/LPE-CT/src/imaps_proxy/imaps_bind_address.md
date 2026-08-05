---
type: Rust Function
title: imaps_bind_address
resource: LPE-CT/src/imaps_proxy.rs#L111-L113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/imaps_proxy/non_empty_env
  called_by:
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) fn imaps_bind_address() -> Option<String>`

# Calls

- [non_empty_env](../../../../functions/LPE-CT/src/imaps_proxy/non_empty_env.md)

# Called by

- [main](../../../../functions/LPE-CT/src/main.md)