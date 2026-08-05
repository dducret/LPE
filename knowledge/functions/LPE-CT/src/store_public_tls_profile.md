---
type: Rust Function
title: store_public_tls_profile
resource: LPE-CT/src/main.rs#L1048-L1090
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/validate_tls_pair_from_pem
  - functions/LPE-CT/src/public_tls_store_dir
  - functions/LPE-CT/src/normalize_pem_text
  - functions/LPE-CT/src/write_private_key_file
  called_by:
  - functions/LPE-CT/src/http_routes/upload_public_tls_profile
---

# Signature

`fn store_public_tls_profile( state: &AppState, payload: PublicTlsUploadRequest, ) -> Result<(PublicTlsProfile, bool)>`

# Calls

- [validate_tls_pair_from_pem](../../../functions/LPE-CT/src/validate_tls_pair_from_pem.md)
- [public_tls_store_dir](../../../functions/LPE-CT/src/public_tls_store_dir.md)
- [normalize_pem_text](../../../functions/LPE-CT/src/normalize_pem_text.md)
- [write_private_key_file](../../../functions/LPE-CT/src/write_private_key_file.md)

# Called by

- [upload_public_tls_profile](../../../functions/LPE-CT/src/http_routes/upload_public_tls_profile.md)