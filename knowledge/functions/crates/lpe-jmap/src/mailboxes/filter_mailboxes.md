---
type: Rust Function
title: filter_mailboxes
resource: crates/lpe-jmap/src/mailboxes.rs#L631-L668
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes
---

# Signature

`fn filter_mailboxes( mailboxes: Vec<JmapMailbox>, filter: Option<&Value>, ) -> Result<Vec<JmapMailbox>>`

# Calls

- [parse_parent_id_field](../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_mailbox_query](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query.md)
- [handle_mailbox_query_changes](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query_changes.md)