---
type: Rust Method
title: upsert_journal_entry
resource: crates/lpe-admin-api/src/workspace/tests.rs#L240-L261
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn upsert_journal_entry( &self, input: UpsertJournalEntryInput, ) -> anyhow::Result<JournalEntry>`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)