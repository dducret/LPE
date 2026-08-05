---
type: Rust Method
title: fetch_jmap_emails_with_protected_bcc
resource: crates/lpe-storage/src/protocols.rs#L1066-L1084
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/protocols/Storage/fetch_visible_protected_bcc_recipients
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`pub async fn fetch_jmap_emails_with_protected_bcc( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<JmapEmail>>`

# Calls

- [fetch_visible_protected_bcc_recipients](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_visible_protected_bcc_recipients.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)