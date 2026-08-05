---
type: Rust Method
title: fast_transfer_buffer_size
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L292-L309
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata
---

# Signature

`pub(in crate::mapi) fn fast_transfer_buffer_size(&self) -> usize`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [fast_transfer_source_get_buffer_transfer_size](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size.md)
- [fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata](../../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata.md)
- [fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata](../../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata.md)