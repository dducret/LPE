---
type: Rust Method
title: with_navigation_shortcut_identities
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L274-L294
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics
---

# Signature

`pub(crate) fn with_navigation_shortcut_identities( mut self, identity_records: &[MapiIdentityRecord], ) -> Result<Self>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)