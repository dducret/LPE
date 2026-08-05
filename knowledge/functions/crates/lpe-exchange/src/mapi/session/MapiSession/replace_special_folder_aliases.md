---
type: Rust Method
title: replace_special_folder_aliases
resource: crates/lpe-exchange/src/mapi/session.rs#L966-L973
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias
  called_by:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases
---

# Signature

`pub(in crate::mapi) fn replace_special_folder_aliases( &mut self, aliases: impl IntoIterator<Item = (u64, u64)>, )`

# Calls

- [record_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_special_folder_alias.md)

# Called by

- [refresh_persisted_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases.md)