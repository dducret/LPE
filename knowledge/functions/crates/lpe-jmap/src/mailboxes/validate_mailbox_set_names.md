---
type: Rust Function
title: validate_mailbox_set_names
resource: crates/lpe-jmap/src/mailboxes.rs#L670-L765
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_name_field
  - functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  - functions/crates/lpe-jmap/src/mailboxes/mailbox_parent_chain_contains
  - functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set
---

# Signature

`fn validate_mailbox_set_names( create: Option<&HashMap<String, Value>>, update: Option<&HashMap<String, Value>>, existing_mailboxes: &[JmapMailbox], ) -> Result<()>`

# Calls

- [mailbox_name_field](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_name_field.md)
- [parse_parent_id_field](../../../../../functions/crates/lpe-jmap/src/mailboxes/parse_parent_id_field.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_uuid](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)
- [mailbox_parent_chain_contains](../../../../../functions/crates/lpe-jmap/src/mailboxes/mailbox_parent_chain_contains.md)
- [collides_with](../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxCanonicalKey/collides_with.md)

# Called by

- [handle_mailbox_set](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_set.md)