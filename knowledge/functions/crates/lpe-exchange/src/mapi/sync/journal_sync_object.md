---
type: Rust Function
title: journal_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L576-L667
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_size
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
---

# Signature

`fn journal_sync_object( entry: &crate::mapi_store::MapiJournalEntry, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [journal_entry_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_property_value.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [journal_entry_size](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_size.md)

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)