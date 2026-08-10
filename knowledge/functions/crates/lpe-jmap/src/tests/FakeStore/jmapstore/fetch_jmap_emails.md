---
type: Rust Method
title: fetch_jmap_emails
resource: crates/lpe-jmap/src/tests.rs#L1149-L1171
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`async fn fetch_jmap_emails(&self, account_id: Uuid, ids: &[Uuid]) -> Result<Vec<JmapEmail>>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)