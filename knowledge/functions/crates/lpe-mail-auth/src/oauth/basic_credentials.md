---
type: Rust Function
title: basic_credentials
resource: crates/lpe-mail-auth/src/oauth.rs#L75-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_account
---

# Signature

`pub fn basic_credentials(headers: &HeaderMap) -> Result<Option<(String, String)>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [authenticate_account](../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)