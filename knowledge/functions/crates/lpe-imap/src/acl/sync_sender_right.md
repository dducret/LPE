---
type: Rust Function
title: sync_sender_right
resource: crates/lpe-imap/src/acl.rs#L475-L519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/acl/Session/apply_acl_update
---

# Signature

`async fn sync_sender_right<S: crate::store::ImapStore>( store: &S, principal: &lpe_mail_auth::AccountPrincipal, mailbox_name: &str, identifier: &str, grantee_account_id: uuid::Uuid, sender_right: SenderDelegationRight, should_exist: bool, exists: bool, ) -> Result<()>`

# Called by

- [apply_acl_update](../../../../../functions/crates/lpe-imap/src/acl/Session/apply_acl_update.md)