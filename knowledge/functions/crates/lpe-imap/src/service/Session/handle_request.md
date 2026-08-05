---
type: Rust Method
title: handle_request
resource: crates/lpe-imap/src/service.rs#L240-L443
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/handle_capability
  - functions/crates/lpe-imap/src/service/Session/handle_noop
  - functions/crates/lpe-imap/src/service/Session/handle_logout
  - functions/crates/lpe-imap/src/auth/Session/handle_login
  - functions/crates/lpe-imap/src/auth/Session/handle_authenticate
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_list
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_xlist
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_subscribe
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_unsubscribe
  - functions/crates/lpe-imap/src/service/Session/handle_id
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_namespace
  - functions/crates/lpe-imap/src/service/Session/handle_enable
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_status
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_create
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_rename
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_select
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_examine
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_check
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_close
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_unselect
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge
  - functions/crates/lpe-imap/src/acl/Session/handle_getacl
  - functions/crates/lpe-imap/src/service/Session/handle_getquotaroot
  - functions/crates/lpe-imap/src/service/Session/handle_getquota
  - functions/crates/lpe-imap/src/acl/Session/handle_myrights
  - functions/crates/lpe-imap/src/acl/Session/handle_listrights
  - functions/crates/lpe-imap/src/acl/Session/handle_setacl
  - functions/crates/lpe-imap/src/acl/Session/handle_deleteacl
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
  - functions/crates/lpe-imap/src/messages/Session/handle_store
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
  - functions/crates/lpe-imap/src/messages/Session/handle_move
  - functions/crates/lpe-imap/src/uid/Session/handle_uid
  - functions/crates/lpe-imap/src/idle/Session/handle_idle
  - functions/crates/lpe-imap/src/append/Session/handle_append
---

# Signature

`pub(crate) async fn handle_request<R, W>( &mut self, reader: &mut BufReader<R>, writer: &mut W, line: &str, ) -> Result<bool> where R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin,`

# Calls

- [handle_capability](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_capability.md)
- [handle_noop](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_noop.md)
- [handle_logout](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_logout.md)
- [handle_login](../../../../../../functions/crates/lpe-imap/src/auth/Session/handle_login.md)
- [handle_authenticate](../../../../../../functions/crates/lpe-imap/src/auth/Session/handle_authenticate.md)
- [handle_list](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_list.md)
- [handle_xlist](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_xlist.md)
- [handle_lsub](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_lsub.md)
- [handle_subscribe](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_subscribe.md)
- [handle_unsubscribe](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_unsubscribe.md)
- [handle_id](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_id.md)
- [handle_namespace](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_namespace.md)
- [handle_enable](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_enable.md)
- [handle_status](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_status.md)
- [handle_create](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_create.md)
- [handle_rename](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_rename.md)
- [handle_select](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select.md)
- [handle_examine](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_examine.md)
- [handle_check](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_check.md)
- [handle_close](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_close.md)
- [handle_unselect](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_unselect.md)
- [handle_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_expunge.md)
- [handle_getacl](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_getacl.md)
- [handle_getquotaroot](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_getquotaroot.md)
- [handle_getquota](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_getquota.md)
- [handle_myrights](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_myrights.md)
- [handle_listrights](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_listrights.md)
- [handle_setacl](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_setacl.md)
- [handle_deleteacl](../../../../../../functions/crates/lpe-imap/src/acl/Session/handle_deleteacl.md)
- [handle_fetch](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)
- [handle_store](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)
- [handle_copy](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)
- [handle_uid](../../../../../../functions/crates/lpe-imap/src/uid/Session/handle_uid.md)
- [handle_idle](../../../../../../functions/crates/lpe-imap/src/idle/Session/handle_idle.md)
- [handle_append](../../../../../../functions/crates/lpe-imap/src/append/Session/handle_append.md)