---
type: Rust Method
title: upsert_jmap_journal_entry
resource: crates/lpe-jmap/src/tests.rs#L2183-L2205
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn upsert_jmap_journal_entry( &self, input: UpsertJournalEntryInput, ) -> Result<JournalEntry>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)