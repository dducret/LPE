---
type: Rust Function
title: content_length_header
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L119-L126
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/ping_response
---

# Signature

`pub(in crate::mapi) fn content_length_header(headers: &HeaderMap) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [ping_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/ping_response.md)