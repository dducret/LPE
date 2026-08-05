---
type: Rust Function
title: search_folder_definition_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L753-L809
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object
---

# Signature

`fn search_folder_definition_sync_object( message: &SearchFolderDefinition, account_id: Uuid, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [search_folder_definition_message_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [as_i64](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)
- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)

# Called by

- [common_views_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_views_sync_object.md)