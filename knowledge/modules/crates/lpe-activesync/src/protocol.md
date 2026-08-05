---
type: Rust Module
title: protocol
resource: crates/lpe-activesync/src/protocol.rs#L1-L499
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/std-fmt
  - external/super
  member_of:
  - packages/crates/lpe-activesync
---

# Contains

- [ActiveSyncCommand](../../../../classes/crates/lpe-activesync/src/protocol/ActiveSyncCommand.md)
- [as_str](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/as_str.md)
- [from_name](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/from_name.md)
- [from_code](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/from_code.md)
- [known_unsupported_name](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/known_unsupported_name.md)
- [known_unsupported_name_for_str](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/known_unsupported_name_for_str.md)
- [fmt](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/fmt-display/fmt.md)
- [ActiveSyncStatus](../../../../classes/crates/lpe-activesync/src/protocol/ActiveSyncStatus.md)
- [as_str](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncStatus/as_str.md)
- [ActiveSyncFolderType](../../../../classes/crates/lpe-activesync/src/protocol/ActiveSyncFolderType.md)
- [as_str](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncFolderType/as_str.md)
- [from_mailbox_role](../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncFolderType/from_mailbox_role.md)
- [BodyPreferenceType](../../../../classes/crates/lpe-activesync/src/protocol/BodyPreferenceType.md)
- [as_str](../../../../functions/crates/lpe-activesync/src/protocol/BodyPreferenceType/as_str.md)
- [from_u8](../../../../functions/crates/lpe-activesync/src/protocol/BodyPreferenceType/from_u8.md)
- [WbxmlCodePage](../../../../classes/crates/lpe-activesync/src/protocol/WbxmlCodePage.md)
- [as_u8](../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/as_u8.md)
- [try_from](../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [known_unsupported_name](../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/known_unsupported_name.md)
- [from](../../../../functions/crates/lpe-activesync/src/protocol/u8/from-wbxmlcodepage/from.md)
- [activesync_command_codes_match_ms_ashttp](../../../../functions/crates/lpe-activesync/src/protocol/activesync_command_codes_match_ms_ashttp.md)
- [wbxml_code_pages_match_bounded_ms_aswbxml_manifest](../../../../functions/crates/lpe-activesync/src/protocol/wbxml_code_pages_match_bounded_ms_aswbxml_manifest.md)
- [active_sync_status_folder_and_body_values_are_manifest_checked](../../../../functions/crates/lpe-activesync/src/protocol/active_sync_status_folder_and_body_values_are_manifest_checked.md)

# Imports

- `anyhow::{bail, Result}`
- `std::fmt`
- `super::*`

# Member of

- [lpe-activesync](../../../../packages/crates/lpe-activesync.md)