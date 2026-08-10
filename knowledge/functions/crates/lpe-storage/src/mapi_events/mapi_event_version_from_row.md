---
type: Rust Function
title: mapi_event_version_from_row
resource: crates/lpe-storage/src/mapi_events.rs#L1412-L1426
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`fn mapi_event_version_from_row(row: sqlx::postgres::PgRow) -> Result<MapiEventVersion>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)