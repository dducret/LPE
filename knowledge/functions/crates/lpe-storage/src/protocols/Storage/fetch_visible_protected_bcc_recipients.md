---
type: Rust Method
title: fetch_visible_protected_bcc_recipients
resource: crates/lpe-storage/src/protocols.rs#L1086-L1130
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_emails_with_protected_bcc
---

# Signature

`async fn fetch_visible_protected_bcc_recipients( &self, account_id: Uuid, message_ids: &[Uuid], ) -> Result<HashMap<Uuid, Vec<JmapEmailAddress>>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [fetch_jmap_emails_with_protected_bcc](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_emails_with_protected_bcc.md)