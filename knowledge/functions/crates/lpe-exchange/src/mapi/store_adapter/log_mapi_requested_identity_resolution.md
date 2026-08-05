---
type: Rust Function
title: log_mapi_requested_identity_resolution
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L994-L1045
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn log_mapi_requested_identity_resolution( account_id: Uuid, plan: &MapiAccessPlan, identities: &[MapiIdentityLookupRecord], )`

# Calls

- [is_expected_unbacked_mapi_object](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/is_expected_unbacked_mapi_object.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)