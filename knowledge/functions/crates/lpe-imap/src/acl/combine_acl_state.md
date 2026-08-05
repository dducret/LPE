---
type: Rust Function
title: combine_acl_state
resource: crates/lpe-imap/src/acl.rs#L337-L370
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-imap/src/acl/Session/handle_getacl
  - functions/crates/lpe-imap/src/acl/Session/apply_acl_update
---

# Signature

`fn combine_acl_state( mailbox_grants: &[lpe_storage::MailboxDelegationGrant], sender_grants: &[lpe_storage::SenderDelegationGrant], ) -> BTreeMap<String, AclState>`

# Calls

- [entry](../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [handle_getacl](../../../../../functions/crates/lpe-imap/src/acl/Session/handle_getacl.md)
- [apply_acl_update](../../../../../functions/crates/lpe-imap/src/acl/Session/apply_acl_update.md)