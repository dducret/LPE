---
type: Rust Function
title: format_inbox_fai_handoff_visibility_context
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L600-L647
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/outlook_configuration_prefix_debug_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_fai_handoff_visibility_context_separates_prefix_and_named_view_rows
---

# Signature

`pub(super) fn format_inbox_fai_handoff_visibility_context( snapshot: &MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, account_id: Uuid, ) -> String`

# Calls

- [debug_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [outlook_configuration_prefix_debug_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/outlook_configuration_prefix_debug_restriction.md)
- [debug_advertised_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)

# Called by

- [inbox_fai_handoff_visibility_context_separates_prefix_and_named_view_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_fai_handoff_visibility_context_separates_prefix_and_named_view_rows.md)