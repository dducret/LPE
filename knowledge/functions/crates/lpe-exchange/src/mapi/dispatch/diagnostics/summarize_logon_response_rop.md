---
type: Rust Function
title: summarize_logon_response_rop
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L798-L857
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_special_folder_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_issues
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_guid_le
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_u64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/logon_response_debug_summary_decodes_private_mailbox_fields
---

# Signature

`pub(super) fn summarize_logon_response_rop( rop_buffer: &[u8], request_rop_ids: &[u8], ) -> LogonResponseDebugSummary`

# Calls

- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [format_logon_special_folder_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_logon_special_folder_contract.md)
- [logon_special_folder_contract_issues](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/logon_special_folder_contract_issues.md)
- [read_guid_le](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_guid_le.md)
- [read_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [read_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_u64.md)

# Called by

- [log_execute_rop_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [logon_response_debug_summary_decodes_private_mailbox_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/logon_response_debug_summary_decodes_private_mailbox_fields.md)