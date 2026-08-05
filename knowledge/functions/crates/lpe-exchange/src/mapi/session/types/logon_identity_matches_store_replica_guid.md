---
type: Rust Function
title: logon_identity_matches_store_replica_guid
resource: crates/lpe-exchange/src/mapi/session/types.rs#L67-L78
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn logon_identity_matches_store_replica_guid( logon_mailbox_guid: &str, logon_replica_guid: &str, account_id: Uuid, store_replica_guid: Option<Uuid>, ) -> Option<bool>`

# Calls

- [hex_preview](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)

# Called by

- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)