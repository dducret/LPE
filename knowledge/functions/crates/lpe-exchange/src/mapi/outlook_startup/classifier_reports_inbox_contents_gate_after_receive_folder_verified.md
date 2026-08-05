---
type: Rust Function
title: classifier_reports_inbox_contents_gate_after_receive_folder_verified
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L408-L420
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
---

# Signature

`fn classifier_reports_inbox_contents_gate_after_receive_folder_verified()`

# Calls

- [test_session_for_outlook_startup](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/test_session_for_outlook_startup.md)
- [record_receive_folder_verification_passed](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_receive_folder_verification_passed.md)
- [outlook_startup_gate_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)