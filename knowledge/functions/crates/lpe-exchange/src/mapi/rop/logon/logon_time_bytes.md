---
type: Rust Function
title: logon_time_bytes
resource: crates/lpe-exchange/src/mapi/rop/logon.rs#L74-L97
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/civil_from_unix_days
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/rop/tests/logon_time_bytes_encode_valid_utc_calendar_fields
---

# Signature

`pub(in crate::mapi) fn logon_time_bytes(now: SystemTime) -> [u8; 8]`

# Calls

- [civil_from_unix_days](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/civil_from_unix_days.md)

# Called by

- [rop_logon_response_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body.md)
- [rop_public_folder_logon_response_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body.md)
- [logon_time_bytes_encode_valid_utc_calendar_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/logon_time_bytes_encode_valid_utc_calendar_fields.md)