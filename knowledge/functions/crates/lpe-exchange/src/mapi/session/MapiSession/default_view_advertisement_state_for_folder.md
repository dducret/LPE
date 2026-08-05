---
type: Rust Method
title: default_view_advertisement_state_for_folder
resource: crates/lpe-exchange/src/mapi/session.rs#L811-L826
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/format_default_view_advertisement_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_preserves_matching_open_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders
---

# Signature

`pub(in crate::mapi) fn default_view_advertisement_state_for_folder( &self, owner_folder_id: u64, ) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [format_default_view_advertisement_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/format_default_view_advertisement_state.md)

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [default_view_advertisement_preserves_matching_open_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_preserves_matching_open_state.md)
- [default_view_advertisement_state_tracks_multiple_owner_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders.md)