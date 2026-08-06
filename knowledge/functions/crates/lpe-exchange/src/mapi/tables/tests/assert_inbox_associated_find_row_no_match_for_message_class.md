---
type: Rust Function
title: assert_inbox_associated_find_row_no_match_for_message_class
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8552-L8561
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_response_for_message_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_suppresses_outlook_eas_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_elc_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unpersisted_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_sharing_configuration
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_sharing_index
---

# Signature

`fn assert_inbox_associated_find_row_no_match_for_message_class(message_class: &str)`

# Calls

- [inbox_associated_find_row_response_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_response_for_message_class.md)

# Called by

- [inbox_associated_find_row_suppresses_outlook_eas_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_suppresses_outlook_eas_config.md)
- [inbox_associated_find_row_returns_not_found_for_unstored_elc_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_elc_config.md)
- [inbox_associated_find_row_returns_not_found_for_unpersisted_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unpersisted_named_view.md)
- [inbox_associated_find_row_returns_not_found_for_unstored_sharing_configuration](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_sharing_configuration.md)
- [inbox_associated_find_row_returns_not_found_for_unstored_sharing_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_sharing_index.md)