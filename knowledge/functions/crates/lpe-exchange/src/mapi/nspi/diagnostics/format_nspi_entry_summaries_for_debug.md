---
type: Rust Function
title: format_nspi_entry_summaries_for_debug
resource: crates/lpe-exchange/src/mapi/nspi/diagnostics.rs#L305-L327
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_entry_debug_summary_includes_mid_kind_email_and_name
---

# Signature

`pub(super) fn format_nspi_entry_summaries_for_debug( account_id: Uuid, entries: &[ExchangeAddressBookEntry], ) -> String`

# Called by

- [nspi_entry_debug_summary_includes_mid_kind_email_and_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_entry_debug_summary_includes_mid_kind_email_and_name.md)