---
type: Rust Method
title: hierarchy_sync_completed
resource: crates/lpe-exchange/src/mapi/session.rs#L1061-L1065
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_submit_attempt_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_setprops_contract
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logoff_after_hierarchy_completion
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion
---

# Signature

`pub(in crate::mapi) fn hierarchy_sync_completed(&self) -> bool`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [record_last_post_hierarchy_create_save_object_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context.md)
- [record_post_hierarchy_submit_attempt_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_submit_attempt_context.md)
- [record_post_hierarchy_request_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_request_contract.md)
- [record_post_hierarchy_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_getprops_contract.md)
- [record_post_hierarchy_setprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_hierarchy_setprops_contract.md)
- [record_logoff_after_hierarchy_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logoff_after_hierarchy_completion.md)
- [record_execute_after_hierarchy_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion.md)