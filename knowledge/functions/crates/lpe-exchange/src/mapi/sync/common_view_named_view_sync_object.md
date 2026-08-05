---
type: Rust Function
title: common_view_named_view_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L811-L853
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_view_named_view_sync_projects_canonical_descriptor_properties
---

# Signature

`fn common_view_named_view_sync_object( message: &crate::mapi_store::MapiCommonViewNamedViewMessage, account_id: Uuid, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [common_view_named_view_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)

# Called by

- [common_views_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object.md)
- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)
- [common_view_named_view_sync_projects_canonical_descriptor_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_view_named_view_sync_projects_canonical_descriptor_properties.md)