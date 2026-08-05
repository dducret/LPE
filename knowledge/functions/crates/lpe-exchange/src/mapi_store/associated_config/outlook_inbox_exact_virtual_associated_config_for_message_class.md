---
type: Rust Function
title: outlook_inbox_exact_virtual_associated_config_for_message_class
resource: crates/lpe-exchange/src/mapi_store/associated_config.rs#L361-L379
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_defaults
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_exact_virtual_inbox_debug_associated_config
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/virtual_umolk_user_options_reports_optional_properties_absent
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_id
---

# Signature

`pub(crate) fn outlook_inbox_exact_virtual_associated_config_for_message_class( message_class: &str, ) -> Option<MapiAssociatedConfigMessage>`

# Calls

- [outlook_inbox_associated_config_defaults](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_defaults.md)

# Called by

- [append_exact_virtual_inbox_debug_associated_config](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/append_exact_virtual_inbox_debug_associated_config.md)
- [associated_table_rows_with_lookup_restriction](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_rows_with_lookup_restriction.md)
- [virtual_umolk_user_options_reports_optional_properties_absent](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/virtual_umolk_user_options_reports_optional_properties_absent.md)
- [outlook_inbox_exact_virtual_associated_config_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_id.md)