---
type: Rust Function
title: parse_response_number_after
resource: crates/lpe-imap/src/tests.rs#L4031-L4037
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-imap/src/tests/outlook_first_login_list_select_sync_transcript
---

# Signature

`fn parse_response_number_after(value: &str, marker: &str) -> usize`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [outlook_first_login_list_select_sync_transcript](../../../../../functions/crates/lpe-imap/src/tests/outlook_first_login_list_select_sync_transcript.md)