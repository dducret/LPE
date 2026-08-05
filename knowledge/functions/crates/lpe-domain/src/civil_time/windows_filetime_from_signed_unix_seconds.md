---
type: Rust Function
title: windows_filetime_from_signed_unix_seconds
resource: crates/lpe-domain/src/civil_time.rs#L62-L67
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime
---

# Signature

`pub fn windows_filetime_from_signed_unix_seconds(unix_seconds: i64) -> u64`

# Called by

- [parse_rfc3339_utc_filetime](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime.md)