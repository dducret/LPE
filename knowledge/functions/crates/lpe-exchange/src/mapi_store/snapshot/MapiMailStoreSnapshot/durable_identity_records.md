---
type: Rust Method
title: durable_identity_records
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L95-L97
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot
---

# Signature

`pub(crate) fn durable_identity_records(&self) -> &[MapiIdentityRecord]`

# Called by

- [finalize_mapi_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot.md)