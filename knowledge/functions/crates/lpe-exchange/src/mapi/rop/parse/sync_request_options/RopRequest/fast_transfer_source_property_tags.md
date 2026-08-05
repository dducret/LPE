---
type: Rust Method
title: fast_transfer_source_property_tags
resource: crates/lpe-exchange/src/mapi/rop/parse/sync_request_options.rs#L42-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response
---

# Signature

`pub(in crate::mapi) fn fast_transfer_source_property_tags(&self) -> Vec<u32>`

# Calls

- [get](../../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_fast_transfer_source_copy_response](../../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response.md)