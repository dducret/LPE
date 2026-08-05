---
type: Rust Function
title: remember_created_message_mapi_identity
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1388-L1443
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn remember_created_message_mapi_identity<S>( store: &S, principal: &AccountPrincipal, canonical_id: Uuid, source_key: Option<Vec<u8>>, ) -> Result<(crate::store::MapiIdentityRecord, bool, String)> where S: ExchangeStore,`

# Calls

- [remember_created_mapi_identity_record](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)