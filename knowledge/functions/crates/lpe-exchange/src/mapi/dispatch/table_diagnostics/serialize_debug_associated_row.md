---
type: Rust Function
title: serialize_debug_associated_row
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L551-L564
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
---

# Signature

`pub(super) fn serialize_debug_associated_row( message: &DebugAssociatedTableRow, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_associated_config_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid.md)
- [serialize_common_view_named_view_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid.md)

# Called by

- [format_inbox_associated_wire_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)