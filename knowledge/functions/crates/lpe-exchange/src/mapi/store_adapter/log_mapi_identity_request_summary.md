---
type: Rust Function
title: log_mapi_identity_request_summary
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L1151-L1213
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/mapi_identity_kind_name
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn log_mapi_identity_request_summary( account_id: Uuid, plan: &MapiAccessPlan, request_set: &'static str, raw_count: usize, requests: &[MapiIdentityRequest], )`

# Calls

- [mapi_identity_kind_name](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/mapi_identity_kind_name.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)