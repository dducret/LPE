---
type: Rust Function
title: serialize_common_views_property_row_for_principal
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L45-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_optional_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn serialize_common_views_property_row_for_principal( message: &MapiCommonViewsMessage, principal: &AccountPrincipal, columns: &[u32], ) -> Vec<u8>`

# Calls

- [serialize_optional_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_optional_property_row.md)
- [common_views_message_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal.md)

# Called by

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)