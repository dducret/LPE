---
type: Rust Function
title: inbox_associated_exact_configuration_findrow_matched
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L964-L986
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
---

# Signature

`pub(super) fn inbox_associated_exact_configuration_findrow_matched( object: Option<&MapiObject>, request: &RopRequest, response: &[u8], ) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction.md)

# Called by

- [append_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)