---
type: Rust Function
title: release_handle_slot
resource: crates/lpe-exchange/src/mapi/session.rs#L1375-L1393
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/forget_table_notification_handle
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/session/tests/release_handle_slot_forgets_folder_profile_property_tombstones
---

# Signature

`pub(in crate::mapi) fn release_handle_slot( session: &mut MapiSession, handle_slots: &mut [u32], request: &RopRequest, )`

# Calls

- [input_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [forget_table_notification_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/forget_table_notification_handle.md)
- [remove](../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)

# Called by

- [append_release_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [release_handle_slot_forgets_folder_profile_property_tombstones](../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/release_handle_slot_forgets_folder_profile_property_tombstones.md)