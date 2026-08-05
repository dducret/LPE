---
type: Rust Function
title: classify_outlook_bootstrap_row_invariants
resource: crates/lpe-exchange/src/mapi/tables/diagnostics.rs#L215-L330
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/binary_property
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/u64_property
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/string_property
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/u32_property
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/count_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/tests/outlook_bootstrap_row_invariant_classifier_reports_consistency
  - functions/crates/lpe-exchange/src/mapi/tables/tests/outlook_bootstrap_row_invariant_classifier_flags_missing_record_key
---

# Signature

`pub(super) fn classify_outlook_bootstrap_row_invariants<F>( row_index: usize, row_kind: &str, object_id: u64, expected_folder_id: Option<u64>, expected_parent_id: Option<u64>, expected_container_class: Option<&str>, mut value: F, ) -> String where F: FnMut(u32) -> Option<MapiValue>,`

# Calls

- [binary_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/binary_property.md)
- [u64_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/u64_property.md)
- [string_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/string_property.md)
- [u32_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/u32_property.md)
- [count_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/count_property.md)

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [outlook_bootstrap_row_invariant_classifier_reports_consistency](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/outlook_bootstrap_row_invariant_classifier_reports_consistency.md)
- [outlook_bootstrap_row_invariant_classifier_flags_missing_record_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/outlook_bootstrap_row_invariant_classifier_flags_missing_record_key.md)