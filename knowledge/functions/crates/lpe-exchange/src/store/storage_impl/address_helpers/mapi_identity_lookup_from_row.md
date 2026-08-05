---
type: Rust Function
title: mapi_identity_lookup_from_row
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L118-L142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn mapi_identity_lookup_from_row(row: sqlx::postgres::PgRow) -> Result<MapiIdentityLookupRecord>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)