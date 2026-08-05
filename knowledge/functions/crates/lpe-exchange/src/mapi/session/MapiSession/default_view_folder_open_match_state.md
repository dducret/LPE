---
type: Rust Method
title: default_view_folder_open_match_state
resource: crates/lpe-exchange/src/mapi/session.rs#L841-L885
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders
---

# Signature

`pub(in crate::mapi) fn default_view_folder_open_match_state( &self, opened_folder_id: u64, default_view_target: Option<(u64, u64)>, ) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [default_view_advertisement_state_marks_matching_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open.md)
- [default_view_match_state_reports_pre_advertised_folder_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open.md)
- [default_view_advertisement_state_tracks_multiple_owner_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders.md)