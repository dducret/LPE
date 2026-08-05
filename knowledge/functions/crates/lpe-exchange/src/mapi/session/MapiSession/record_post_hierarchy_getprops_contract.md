---
type: Rust Method
title: record_post_hierarchy_getprops_contract
resource: crates/lpe-exchange/src/mapi/session.rs#L939-L947
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_request_contracts
---

# Signature

`pub(in crate::mapi) fn record_post_hierarchy_getprops_contract(&mut self, contract: String)`

# Calls

- [hierarchy_sync_completed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [post_hierarchy_action_summary_records_last_request_contracts](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_request_contracts.md)