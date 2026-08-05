---
type: Rust Function
title: host_header
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L110-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
---

# Signature

`pub(in crate::mapi) fn host_header(headers: &HeaderMap) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)