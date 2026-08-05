---
type: Rust Method
title: record_completed_hierarchy_sync
resource: crates/lpe-exchange/src/mapi/session.rs#L1067-L1079
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_sync_observations
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_create_save_object
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_submit_attempt_context
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_request_contracts
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_release
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_open
  - functions/crates/lpe-exchange/src/mapi/transport/tests/required_default_folder_disconnect_coverage_reports_calendar_contacts_gap
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe
---

# Signature

`pub(in crate::mapi) fn record_completed_hierarchy_sync( &mut self, sync_root_folder_id: u64, get_buffer_summary: String, default_folder_membership_summary: String, )`

# Called by

- [record_execute_sync_observations](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/record_execute_sync_observations.md)
- [post_hierarchy_action_summary_records_execute_rops_and_client_actions](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions.md)
- [post_hierarchy_action_summary_records_last_create_save_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_create_save_object.md)
- [post_hierarchy_action_summary_records_submit_attempt_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_submit_attempt_context.md)
- [post_hierarchy_action_summary_records_last_request_contracts](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_request_contracts.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_release.md)
- [post_hierarchy_summary_tracks_create_save_after_visible_inbox_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_tracks_create_save_after_visible_inbox_open.md)
- [required_default_folder_disconnect_coverage_reports_calendar_contacts_gap](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/required_default_folder_disconnect_coverage_reports_calendar_contacts_gap.md)
- [post_hierarchy_action_summary_classifies_release_logoff_without_content_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync.md)
- [post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe.md)