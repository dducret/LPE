---
type: Rust Function
title: post_hierarchy_action_summary_classifies_release_logoff_without_content_sync
resource: crates/lpe-exchange/src/mapi/transport/tests.rs#L1680-L1701
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_hierarchy_sync
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logoff_after_hierarchy_completion
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary
---

# Signature

`fn post_hierarchy_action_summary_classifies_release_logoff_without_content_sync()`

# Calls

- [record_completed_hierarchy_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_hierarchy_sync.md)
- [record_execute_after_hierarchy_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_execute_after_hierarchy_completion.md)
- [record_logoff_after_hierarchy_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_logoff_after_hierarchy_completion.md)
- [post_hierarchy_action_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary.md)