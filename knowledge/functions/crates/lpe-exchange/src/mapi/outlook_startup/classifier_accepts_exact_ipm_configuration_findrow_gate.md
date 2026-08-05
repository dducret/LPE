---
type: Rust Function
title: classifier_accepts_exact_ipm_configuration_findrow_gate
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L367-L385
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_exact_findrow
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
---

# Signature

`fn classifier_accepts_exact_ipm_configuration_findrow_gate()`

# Calls

- [test_session_for_outlook_startup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup.md)
- [record_opened_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_opened_folder.md)
- [record_inbox_associated_contents_table](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_contents_table.md)
- [record_inbox_associated_exact_findrow](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_exact_findrow.md)
- [outlook_startup_gate_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)