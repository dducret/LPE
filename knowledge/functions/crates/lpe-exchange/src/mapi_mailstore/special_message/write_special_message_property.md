---
type: Rust Function
title: write_special_message_property
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L439-L487
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_property_info
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i64
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content
---

# Signature

`pub(super) fn write_special_message_property( buffer: &mut Vec<u8>, object: &SpecialMessageSyncFact, property_tag: u32, value: &SpecialMessagePropertyValue, ) -> bool`

# Calls

- [write_fast_transfer_property_info](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_property_info.md)
- [write_i32](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32.md)
- [write_i64](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i64.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [write_fast_transfer_special_message_content](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_fast_transfer_special_message_content.md)