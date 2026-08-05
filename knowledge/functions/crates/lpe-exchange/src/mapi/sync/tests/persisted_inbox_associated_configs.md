---
type: Rust Function
title: persisted_inbox_associated_configs
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L143-L195
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
---

# Signature

`fn persisted_inbox_associated_configs( account_id: Uuid, ) -> Vec<crate::store::MapiAssociatedConfigRecord>`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)

# Called by

- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)