---
type: Rust Function
title: days_in_month
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L395-L403
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime
---

# Signature

`fn days_in_month(year: i32, month: i32) -> Option<i32>`

# Called by

- [parse_rfc3339_utc_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime.md)