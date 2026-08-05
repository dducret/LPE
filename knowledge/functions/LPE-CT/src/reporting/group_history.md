---
type: Rust Function
title: group_history
resource: LPE-CT/src/reporting.rs#L843-L855
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/reporting/search_mail_history
---

# Signature

`fn group_history(events: Vec<MailHistoryEvent>) -> BTreeMap<String, Vec<MailHistoryEvent>>`

# Calls

- [entry](../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [search_mail_history](../../../../functions/LPE-CT/src/reporting/search_mail_history.md)