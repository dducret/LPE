---
type: Rust Function
title: format_common_views_wlink_target_decoding
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1144-L1252
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_target_decoding_reports_inbox_match
---

# Signature

`pub(super) fn format_common_views_wlink_target_decoding( principal: &AccountPrincipal, snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [principal_mailbox_store_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/principal_mailbox_store_entry_id.md)
- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [navigation_shortcut_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value.md)
- [navigation_shortcut_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal.md)

# Called by

- [common_views_wlink_target_decoding_reports_inbox_match](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_target_decoding_reports_inbox_match.md)