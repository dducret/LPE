---
type: Rust Method
title: shared_mailbox_read_only_access
resource: crates/lpe-jmap/src/tests.rs#L788-L795
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/email_set_rejects_read_only_shared_mailbox_mutations
  - functions/crates/lpe-jmap/src/tests/mailbox_get_hides_child_creation_for_read_only_shared_mailboxes
  - functions/crates/lpe-jmap/src/tests/mailbox_get_rename_and_delete_rights_match_mailbox_set
  - functions/crates/lpe-jmap/src/tests/mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations
---

# Signature

`fn shared_mailbox_read_only_access( may_send_as: bool, may_send_on_behalf: bool, ) -> MailboxAccountAccess`

# Called by

- [email_set_rejects_read_only_shared_mailbox_mutations](../../../../../../functions/crates/lpe-jmap/src/tests/email_set_rejects_read_only_shared_mailbox_mutations.md)
- [mailbox_get_hides_child_creation_for_read_only_shared_mailboxes](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_get_hides_child_creation_for_read_only_shared_mailboxes.md)
- [mailbox_get_rename_and_delete_rights_match_mailbox_set](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_get_rename_and_delete_rights_match_mailbox_set.md)
- [mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations](../../../../../../functions/crates/lpe-jmap/src/tests/mailbox_copy_and_import_reject_read_only_shared_mailbox_mutations.md)