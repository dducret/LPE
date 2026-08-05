---
type: Rust Function
title: parse_digits
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L405-L410
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime
---

# Signature

`fn parse_digits(bytes: &[u8]) -> Option<u32>`

# Called by

- [parse_rfc3339_utc_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime.md)