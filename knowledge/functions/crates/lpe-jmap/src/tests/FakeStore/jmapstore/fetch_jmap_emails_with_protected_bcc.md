---
type: Rust Method
title: fetch_jmap_emails_with_protected_bcc
resource: crates/lpe-jmap/src/tests.rs#L1173-L1195
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`async fn fetch_jmap_emails_with_protected_bcc( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<JmapEmail>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)