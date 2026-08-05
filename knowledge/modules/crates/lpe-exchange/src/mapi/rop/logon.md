---
type: Rust Module
title: logon
resource: crates/lpe-exchange/src/mapi/rop/logon.rs#L1-L111
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-write-object-id-write-u32-write-u64-roprequest
  - external/crate-mapi-identity-store-replica-id
  - external/crate-mapi-sync-private-logon-special-folder-ids-public-logon-special-folder-ids
  - external/crate-mapi-accountprincipal
  - external/std-time-duration-systemtime
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [private_logon_response_logon_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/private_logon_response_logon_flags.md)
- [public_folder_logon_response_logon_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/public_folder_logon_response_logon_flags.md)
- [rop_logon_response_body](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body.md)
- [rop_public_folder_logon_response_body](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body.md)
- [gwart_time_marker](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/gwart_time_marker.md)
- [logon_time_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/logon_time_bytes.md)
- [civil_from_unix_days](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/civil_from_unix_days.md)

# Imports

- `super::{write_object_id, write_u32, write_u64, RopRequest}`
- `crate::mapi::identity::STORE_REPLICA_ID`
- `crate::mapi::sync::{PRIVATE_LOGON_SPECIAL_FOLDER_IDS, PUBLIC_LOGON_SPECIAL_FOLDER_IDS}`
- `crate::mapi::AccountPrincipal`
- `std::time::{Duration, SystemTime}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)