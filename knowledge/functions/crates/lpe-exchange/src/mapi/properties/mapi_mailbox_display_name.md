---
type: Rust Function
title: mapi_mailbox_display_name
resource: crates/lpe-exchange/src/mapi/properties.rs#L625-L635
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
---

# Signature

`pub(in crate::mapi) fn mapi_mailbox_display_name(mailbox: &JmapMailbox) -> String`

# Called by

- [debug_open_folder_metadata](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [mailbox_property_value_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [serialize_folder_row_with_context](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)