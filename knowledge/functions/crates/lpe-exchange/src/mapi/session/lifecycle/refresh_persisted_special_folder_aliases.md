---
type: Rust Function
title: refresh_persisted_special_folder_aliases
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L265-L279
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_special_folder_aliases
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/replace_special_folder_aliases
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
---

# Signature

`pub(in crate::mapi) async fn refresh_persisted_special_folder_aliases<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, ) -> Result<()>`

# Calls

- [fetch_mapi_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_special_folder_aliases.md)
- [replace_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/replace_special_folder_aliases.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)