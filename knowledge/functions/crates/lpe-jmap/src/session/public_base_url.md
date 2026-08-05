---
type: Rust Function
title: public_base_url
resource: crates/lpe-jmap/src/session.rs#L81-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/session_handler
---

# Signature

`pub(crate) fn public_base_url(headers: &HeaderMap) -> Option<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [session_handler](../../../../../functions/crates/lpe-jmap/src/service/session_handler.md)