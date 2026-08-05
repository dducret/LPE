---
type: Rust Function
title: get_reminders_response
resource: crates/lpe-exchange/src/service/ews/reminders.rs#L156-L198
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/reminders/reminder_item_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/get_reminders
---

# Signature

`pub(in crate::service) fn get_reminders_response(reminders: &[ClientReminder]) -> String`

# Calls

- [reminder_item_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/reminders/reminder_item_id.md)

# Called by

- [get_reminders](../../../../../../../functions/crates/lpe-exchange/src/service/ews/reminders/ExchangeService/get_reminders.md)