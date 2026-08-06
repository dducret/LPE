---
type: Rust Function
title: format_common_view_descriptor_response_values
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L1131-L1152
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/common_view_descriptor_property_requested
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract
---

# Signature

`fn format_common_view_descriptor_response_values( account_id: uuid::Uuid, message: &crate::mapi_store::MapiCommonViewNamedViewMessage, columns: &[u32], ) -> String`

# Calls

- [common_view_descriptor_property_requested](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/common_view_descriptor_property_requested.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)

# Called by

- [log_common_view_descriptor_getprops_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_common_view_descriptor_getprops_summary.md)
- [format_common_view_descriptor_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_getprops_contract.md)