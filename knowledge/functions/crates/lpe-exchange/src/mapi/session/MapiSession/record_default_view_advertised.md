---
type: Rust Method
title: record_default_view_advertised
resource: crates/lpe-exchange/src/mapi/session.rs#L669-L724
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_preserves_matching_open_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders
  - functions/crates/lpe-exchange/src/mapi/transport/tests/advertised_default_view_pending_open_is_primary_without_visible_inbox_release
  - functions/crates/lpe-exchange/src/mapi/transport/tests/advertised_default_view_pending_open_is_not_primary_after_visible_inbox_release
---

# Signature

`pub(in crate::mapi) fn record_default_view_advertised( &mut self, request_id: &str, owner_folder_id: u64, view_folder_id: u64, view_message_id: u64, view_name: &str, )`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)
- [default_view_advertisement_state_marks_matching_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open.md)
- [default_view_advertisement_preserves_matching_open_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_preserves_matching_open_state.md)
- [default_view_match_state_reports_pre_advertised_folder_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open.md)
- [default_view_advertisement_state_tracks_multiple_owner_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders.md)
- [advertised_default_view_pending_open_is_primary_without_visible_inbox_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/advertised_default_view_pending_open_is_primary_without_visible_inbox_release.md)
- [advertised_default_view_pending_open_is_not_primary_after_visible_inbox_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/advertised_default_view_pending_open_is_not_primary_after_visible_inbox_release.md)