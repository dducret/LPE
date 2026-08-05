---
type: Rust Function
title: month_abbrev
resource: crates/lpe-domain/src/civil_time.rs#L95-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date
---

# Signature

`pub fn month_abbrev(month: u8) -> Option<&'static str>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [mapi_http_date](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date.md)