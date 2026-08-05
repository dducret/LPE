---
type: Rust Method
title: reload_cached_information_reserved
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L789-L798
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response
---

# Signature

`pub(in crate::mapi) fn reload_cached_information_reserved(&self) -> Option<u16>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_reload_cached_information_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_reload_cached_information_response.md)