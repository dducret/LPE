---
type: Rust Method
title: record_execute_after_hierarchy_completion
resource: crates/lpe-exchange/src/mapi/session.rs#L1099-L1140
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_visible_inbox_release_create_save_batch
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_visible_inbox_open_create_save_batch
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_stays_empty_before_completed_hierarchy
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_release
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_open
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe
---

# Signature

`pub(in crate::mapi) fn record_execute_after_hierarchy_completion( &mut self, rop_ids: &[u8], rop_names: &str, ) -> PostHierarchyExecuteObservation`

# Calls

- [hierarchy_sync_completed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [record_post_visible_inbox_release_create_save_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_post_visible_inbox_release_create_save_batch.md)
- [record_visible_inbox_open_create_save_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_visible_inbox_open_create_save_batch.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [post_hierarchy_action_summary_stays_empty_before_completed_hierarchy](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_stays_empty_before_completed_hierarchy.md)
- [post_hierarchy_action_summary_records_execute_rops_and_client_actions](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_release.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_open.md)
- [post_hierarchy_action_summary_classifies_release_logoff_without_content_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync.md)
- [post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe.md)