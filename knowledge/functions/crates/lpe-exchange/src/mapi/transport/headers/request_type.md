---
type: Rust Function
title: request_type
resource: crates/lpe-exchange/src/mapi/transport/headers.rs#L18-L54
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/handle_mapi
  - functions/crates/lpe-exchange/src/mapi/transport/tests/request_type_recognizes_get_hierarchy_info_as_nspi_request
---

# Signature

`pub(in crate::mapi) fn request_type(headers: &HeaderMap) -> Result<MapiRequestType>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/handle_mapi.md)
- [request_type_recognizes_get_hierarchy_info_as_nspi_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/request_type_recognizes_get_hierarchy_info_as_nspi_request.md)