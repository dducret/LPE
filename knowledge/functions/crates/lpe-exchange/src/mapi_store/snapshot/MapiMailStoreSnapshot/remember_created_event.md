---
type: Rust Method
title: remember_created_event
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L529-L554
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/fallback_event_version
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_mapi_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
---

# Signature

`pub(crate) fn remember_created_event( &mut self, folder_id: u64, event_id: u64, event: AccessibleEvent, attachments: Vec<CalendarEventAttachment>, )`

# Calls

- [fallback_event_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/fallback_event_version.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [calendar_mapi_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_mapi_attachments.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)