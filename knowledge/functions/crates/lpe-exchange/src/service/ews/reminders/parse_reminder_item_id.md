---
type: Rust Function
title: parse_reminder_item_id
resource: crates/lpe-exchange/src/service/ews/reminders.rs#L211-L224
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/perform_reminder_action
---

# Signature

`pub(in crate::service) fn parse_reminder_item_id(id: &str) -> Option<ParsedReminderItemId>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [perform_reminder_action](../../../../../../../functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/perform_reminder_action.md)