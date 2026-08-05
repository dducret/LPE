---
type: Rust Function
title: associated_config_sync_object
resource: crates/lpe-exchange/src/mapi/sync/associated_config.rs#L3-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_standard_sync_tag
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag
  - functions/crates/lpe-exchange/src/mapi/sync/special_message_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_default_sync_tags
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_text_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_direct_fast_transfer_object
---

# Signature

`pub(super) fn associated_config_sync_object( message: &crate::mapi_store::MapiAssociatedConfigMessage, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [associated_config_standard_sync_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_standard_sync_tag.md)
- [is_associated_config_read_only_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag.md)
- [special_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_message_property_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)
- [associated_config_default_sync_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_default_sync_tags.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [associated_config_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_text_property.md)

# Called by

- [common_views_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object.md)
- [associated_config_direct_fast_transfer_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_direct_fast_transfer_object.md)