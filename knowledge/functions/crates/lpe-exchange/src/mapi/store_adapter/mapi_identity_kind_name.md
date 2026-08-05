---
type: Rust Function
title: mapi_identity_kind_name
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1275-L1295
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_identity_request_summary
  - functions/crates/lpe-exchange/src/mapi/store_adapter/format_mapi_identity_kinds
---

# Signature

`fn mapi_identity_kind_name(object_kind: MapiIdentityObjectKind) -> &'static str`

# Called by

- [log_mapi_identity_request_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/log_mapi_identity_request_summary.md)
- [format_mapi_identity_kinds](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/format_mapi_identity_kinds.md)