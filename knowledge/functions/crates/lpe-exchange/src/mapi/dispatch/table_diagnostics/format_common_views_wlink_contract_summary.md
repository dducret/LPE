---
type: Rust Function
title: format_common_views_wlink_contract_summary
resource: crates/lpe-exchange/src/mapi/dispatch/table_diagnostics.rs#L1254-L1319
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/common_views_link_row_expected_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_contract_distinguishes_expected_link_defaults
---

# Signature

`pub(super) fn format_common_views_wlink_contract_summary( selected_columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)
- [property_ids_match](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/property_ids_match.md)
- [common_views_link_row_expected_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_tags/common_views_link_row_expected_default.md)

# Called by

- [common_views_wlink_contract_distinguishes_expected_link_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_contract_distinguishes_expected_link_defaults.md)