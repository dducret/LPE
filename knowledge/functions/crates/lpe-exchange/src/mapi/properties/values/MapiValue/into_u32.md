---
type: Rust Method
title: into_u32
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L544-L572
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_associated_config_0e0b_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract
---

# Signature

`pub(in crate::mapi) fn into_u32(self) -> Option<u32>`

# Calls

- [try_from](../../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [format_ipm_configuration_row_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_ipm_configuration_row_contract.md)
- [ipm_configuration_row_issues](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/ipm_configuration_row_issues.md)
- [restriction_matches_email_with_attachments](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)
- [restriction_matches](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches.md)
- [message_followup_update_from_mapi_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_followup_update_from_mapi_values.md)
- [write_mapi_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [format_associated_config_0e0b_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_associated_config_0e0b_debug.md)
- [format_ipm_configuration_getprops_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract.md)