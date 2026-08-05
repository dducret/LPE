---
type: Rust Function
title: utc_from_unix_seconds
resource: crates/lpe-domain/src/civil_time.rs#L38-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/civil_from_days
  called_by:
  - functions/crates/lpe-domain/src/civil_time/utc_parts_include_weekday_and_month_names
  - functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date
  - functions/crates/lpe-storage/src/storage_backend/s3_timestamp
---

# Signature

`pub fn utc_from_unix_seconds(total_seconds: u64) -> UtcDateTime`

# Calls

- [civil_from_days](../../../../../functions/crates/lpe-domain/src/civil_time/civil_from_days.md)

# Called by

- [utc_parts_include_weekday_and_month_names](../../../../../functions/crates/lpe-domain/src/civil_time/utc_parts_include_weekday_and_month_names.md)
- [mapi_http_date](../../../../../functions/crates/lpe-exchange/src/mapi/transport/mapi_http_date.md)
- [s3_timestamp](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_timestamp.md)