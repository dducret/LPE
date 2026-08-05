---
type: Rust Function
title: format_message_body_getprops_contract
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L1189-L1266
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/is_message_body_debug_tag
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/tests/message_body_getprops_contract_reports_canonical_body_shape
---

# Signature

`pub(in crate::mapi) fn format_message_body_getprops_contract( object: Option<&MapiObject>, columns: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [is_message_body_debug_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/is_message_body_debug_tag.md)
- [search_folder_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)

# Called by

- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [message_body_getprops_contract_reports_canonical_body_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/message_body_getprops_contract_reports_canonical_body_shape.md)