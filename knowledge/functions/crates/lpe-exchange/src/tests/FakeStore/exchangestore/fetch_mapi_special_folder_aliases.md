---
type: Rust Method
title: fetch_mapi_special_folder_aliases
resource: crates/lpe-exchange/src/tests/mod.rs#L6033-L6046
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards
---

# Signature

`fn fetch_mapi_special_folder_aliases<'a>( &'a self, _account_id: Uuid, ) -> StoreFuture<'a, Vec<MapiSpecialFolderAlias>>`

# Called by

- [refresh_persisted_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases.md)
- [postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_special_folder_alias_round_trip_and_identity_collision_guards.md)