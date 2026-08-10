---
type: Rust Method
title: find_submission_account_by_email_in_same_tenant
resource: crates/lpe-storage/src/submission.rs#L1467-L1496
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input
---

# Signature

`pub async fn find_submission_account_by_email_in_same_tenant( &self, reference_account_id: Uuid, email: &str, ) -> Result<Option<SubmissionAccountIdentity>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [build_smtp_submission_input](../../../../../../functions/crates/lpe-admin-api/src/integration/build_smtp_submission_input.md)