---
type: Rust Function
title: associated_config_direct_fast_transfer_object
resource: crates/lpe-exchange/src/mapi/sync/associated_config.rs#L75-L97
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`pub(super) fn associated_config_direct_fast_transfer_object( message: &crate::mapi_store::MapiAssociatedConfigMessage, mailbox_guid: Uuid, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [associated_config_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)