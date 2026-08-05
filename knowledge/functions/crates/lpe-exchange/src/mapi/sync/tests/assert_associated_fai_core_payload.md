---
type: Rust Function
title: assert_associated_fai_core_payload
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L30-L56
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/assert_has_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
---

# Signature

`fn assert_associated_fai_core_payload(item: &mapi_mailstore::ContentTransferFaiItemDebug)`

# Calls

- [assert_has_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/assert_has_tags.md)

# Called by

- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)