---
type: Rust Method
title: remember_event_version
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L491-L499
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_event_versions
---

# Signature

`pub(crate) fn remember_event_version(&mut self, version: MapiEventVersion)`

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [with_event_versions](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_event_versions.md)