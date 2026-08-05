---
type: Rust Function
title: parse_parent_id_field
resource: crates/lpe-jmap/src/mailboxes.rs#L622-L629
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_create
  - functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_update
  - functions/crates/lpe-jmap/src/mailboxes/filter_mailboxes
  - functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names
---

# Signature

`fn parse_parent_id_field(value: Option<&Value>) -> Result<Option<Option<Uuid>>>`

# Calls

- [parse_uuid](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)

# Called by

- [parse_mailbox_create](../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_create.md)
- [parse_mailbox_update](../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_mailbox_update.md)
- [filter_mailboxes](../../../../../functions/crates/lpe-jmap/src/mailboxes/filter_mailboxes.md)
- [validate_mailbox_set_names](../../../../../functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names.md)