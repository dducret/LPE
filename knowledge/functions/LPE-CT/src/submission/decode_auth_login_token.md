---
type: Rust Function
title: decode_auth_login_token
resource: LPE-CT/src/submission.rs#L582-L589
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/parse_auth_login
  - functions/LPE-CT/src/submission/auth_login_token_decodes_base64_value
---

# Signature

`fn decode_auth_login_token(value: &str) -> Result<String>`

# Called by

- [parse_auth_login](../../../../functions/LPE-CT/src/submission/parse_auth_login.md)
- [auth_login_token_decodes_base64_value](../../../../functions/LPE-CT/src/submission/auth_login_token_decodes_base64_value.md)