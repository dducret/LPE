---
type: Rust Function
title: contact_matches_report
resource: crates/lpe-dav/src/report.rs#L83-L104
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/contact_href
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_report
---

# Signature

`pub(crate) fn contact_matches_report(contact: &AccessibleContact, filter: &ReportFilter) -> bool`

# Calls

- [contact_href](../../../../../functions/crates/lpe-dav/src/paths/contact_href.md)

# Called by

- [handle_report](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)