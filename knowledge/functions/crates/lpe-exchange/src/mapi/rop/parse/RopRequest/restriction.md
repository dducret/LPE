---
type: Rust Method
title: restriction
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1064-L1077
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/restriction_property_tags_from_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_broad_findrow_matched
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_exact_configuration_findrow_matched
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
---

# Signature

`pub(in crate::mapi) fn restriction(&self) -> Result<Option<MapiRestriction>>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_mapi_restriction](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction.md)

# Called by

- [append_restrict_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [restriction_property_tags_from_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/restriction_property_tags_from_request.md)
- [inbox_associated_broad_findrow_matched](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_broad_findrow_matched.md)
- [inbox_associated_exact_configuration_findrow_matched](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/inbox_associated_exact_configuration_findrow_matched.md)
- [format_normal_message_find_row_failure_candidates](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)
- [simulate_table_access](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)
- [rop_find_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)