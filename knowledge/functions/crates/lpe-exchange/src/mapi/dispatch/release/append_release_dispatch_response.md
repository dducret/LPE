---
type: Rust Function
title: append_release_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/release.rs#L8-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_release_dispatch_response<S: ExchangeStore>( _store: &S, principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, same_execute_released_handles: &mut HashSet<u32>, post_hierarchy_release_events: &mut Vec<PostHierarchyReleaseDebugEvent>, ) -> bool`

# Calls

- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)