---
type: Rust Method
title: with_contact_commit_times
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L110-L124
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/contact_commit_times_override_the_durable_contact_identity_timestamp
---

# Signature

`pub(crate) fn with_contact_commit_times(mut self, commit_times: Vec<(Uuid, String)>) -> Self`

# Calls

- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [contact_commit_times_override_the_durable_contact_identity_timestamp](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contact_commit_times_override_the_durable_contact_identity_timestamp.md)