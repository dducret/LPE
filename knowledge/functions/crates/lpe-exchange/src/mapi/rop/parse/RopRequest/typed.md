---
type: Rust Method
title: typed
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1334-L1445
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count
  - functions/crates/lpe-exchange/src/mapi/wire/RopId/is_supported_by_dispatch
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_id_is_reserved
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  - functions/crates/lpe-exchange/src/mapi/rop/serialize/serialize_rop_request
---

# Signature

`pub(in crate::mapi) fn typed(&self) -> TypedRopRequest`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [query_forward_read](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [query_row_count](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)
- [is_supported_by_dispatch](../../../../../../../../functions/crates/lpe-exchange/src/mapi/wire/RopId/is_supported_by_dispatch.md)
- [rop_id_is_reserved](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_id_is_reserved.md)

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [summarize_request_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)
- [serialize_rop_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize/serialize_rop_request.md)