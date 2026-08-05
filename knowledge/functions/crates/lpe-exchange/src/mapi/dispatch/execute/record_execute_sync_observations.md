---
type: Rust Function
title: record_execute_sync_observations
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L379-L399
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_hierarchy_sync
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn record_execute_sync_observations( session: &mut MapiSession, completed_hierarchy_sync: Option<(u64, String, String)>, content_sync_configure_observed: bool, )`

# Calls

- [record_completed_hierarchy_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_hierarchy_sync.md)
- [record_content_sync_configure](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_content_sync_configure.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)