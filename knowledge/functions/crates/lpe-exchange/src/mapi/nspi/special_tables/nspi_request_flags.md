---
type: Rust Function
title: nspi_request_flags
resource: crates/lpe-exchange/src/mapi/nspi/special_tables.rs#L151-L156
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response
---

# Signature

`fn nspi_request_flags(request: &[u8]) -> Option<u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [nspi_hierarchy_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/special_tables/nspi_hierarchy_table_response.md)