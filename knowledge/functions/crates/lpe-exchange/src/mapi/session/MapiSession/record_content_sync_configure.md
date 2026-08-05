---
type: Rust Method
title: record_content_sync_configure
resource: crates/lpe-exchange/src/mapi/session.rs#L1081-L1083
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_sync_observations
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure_for_folder
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions
---

# Signature

`pub(in crate::mapi) fn record_content_sync_configure(&mut self)`

# Called by

- [record_execute_sync_observations](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_sync_observations.md)
- [record_content_sync_configure_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure_for_folder.md)
- [post_hierarchy_action_summary_records_execute_rops_and_client_actions](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions.md)