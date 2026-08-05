---
type: Rust Function
title: special_message_delivery_sort_time
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L182-L197
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
---

# Signature

`fn special_message_delivery_sort_time(object: &SpecialMessageSyncFact) -> u64`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [try_from](../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [parse_rfc3339_utc_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)