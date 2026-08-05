---
type: Rust Function
title: serialize_permission_row
resource: crates/lpe-exchange/src/mapi/permissions.rs#L110-L124
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/permissions/permission_member_id
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(in crate::mapi) fn serialize_permission_row( permission: &MapiFolderPermission, columns: &[u32], ) -> Vec<u8>`

# Calls

- [write_u64](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [permission_member_id](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/permission_member_id.md)
- [write_utf16z](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_property_default](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [rop_query_rows_response_inner](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)