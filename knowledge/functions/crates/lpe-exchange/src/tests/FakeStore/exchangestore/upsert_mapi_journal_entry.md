---
type: Rust Method
title: upsert_mapi_journal_entry
resource: crates/lpe-exchange/src/tests/mod.rs#L9468-L9492
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn upsert_mapi_journal_entry<'a>( &'a self, input: UpsertJournalEntryInput, ) -> StoreFuture<'a, JournalEntry>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)