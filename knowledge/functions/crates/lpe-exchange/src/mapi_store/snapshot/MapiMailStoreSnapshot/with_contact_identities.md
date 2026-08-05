---
type: Rust Method
title: with_contact_identities
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L301-L325
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/contact_commit_times_override_the_durable_contact_identity_timestamp
---

# Signature

`pub(crate) fn with_contact_identities( mut self, identity_records: &[MapiIdentityRecord], ) -> Result<Self>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [contact_commit_times_override_the_durable_contact_identity_timestamp](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contact_commit_times_override_the_durable_contact_identity_timestamp.md)