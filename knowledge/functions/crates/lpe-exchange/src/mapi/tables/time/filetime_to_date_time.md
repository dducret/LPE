---
type: Rust Function
title: filetime_to_date_time
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L46-L50
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds
  - functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_date_time
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/filetime_to_rfc3339_utc
---

# Signature

`pub(in crate::mapi) fn filetime_to_date_time(filetime: i64) -> Option<(String, String)>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [filetime_to_unix_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds.md)
- [unix_seconds_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_date_time.md)

# Called by

- [filetime_to_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/filetime_to_rfc3339_utc.md)