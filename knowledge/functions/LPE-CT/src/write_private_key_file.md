---
type: Rust Function
title: write_private_key_file
resource: LPE-CT/src/main.rs#L1100-L1108
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/store_public_tls_profile
---

# Signature

`fn write_private_key_file(path: &Path, value: &str) -> Result<()>`

# Called by

- [store_public_tls_profile](../../../functions/LPE-CT/src/store_public_tls_profile.md)