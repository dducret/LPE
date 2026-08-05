---
type: Rust Function
title: calendar_event_versions_required
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L913-L933
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn calendar_event_versions_required( plan: &MapiAccessPlan, identities: &[MapiIdentityLookupRecord], ) -> bool`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)