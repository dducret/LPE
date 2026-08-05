---
type: Rust Method
title: upsert_sender_delegation_grant
resource: crates/lpe-imap/src/tests.rs#L917-L953
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/fake_grantee_account_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn upsert_sender_delegation_grant<'a>( &'a self, input: SenderDelegationGrantInput, _audit: AuditEntryInput, ) -> StoreFuture<'a, SenderDelegationGrant>`

# Calls

- [fake_grantee_account_id](../../../../../../../functions/crates/lpe-imap/src/tests/fake_grantee_account_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)