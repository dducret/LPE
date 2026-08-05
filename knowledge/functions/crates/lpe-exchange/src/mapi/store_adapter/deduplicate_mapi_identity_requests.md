---
type: Rust Function
title: deduplicate_mapi_identity_requests
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1136-L1149
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/deduplicate_mapi_identity_requests_keeps_distinct_kinds
---

# Signature

`fn deduplicate_mapi_identity_requests( requests: Vec<MapiIdentityRequest>, ) -> Vec<MapiIdentityRequest>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [deduplicate_mapi_identity_requests_keeps_distinct_kinds](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/deduplicate_mapi_identity_requests_keeps_distinct_kinds.md)