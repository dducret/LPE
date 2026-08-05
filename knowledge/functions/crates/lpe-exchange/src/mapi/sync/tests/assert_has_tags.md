---
type: Rust Function
title: assert_has_tags
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L58-L67
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/assert_associated_fai_core_payload
  - functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
---

# Signature

`fn assert_has_tags(item: &mapi_mailstore::ContentTransferFaiItemDebug, tags: &[u32])`

# Called by

- [assert_associated_fai_core_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_associated_fai_core_payload.md)
- [outlook_inbox_fai_ics_omits_unsupported_message_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties.md)
- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)