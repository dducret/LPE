---
type: Rust Function
title: contact_href
resource: crates/lpe-dav/src/paths.rs#L51-L53
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/propfind/contact_resource_entry
  - functions/crates/lpe-dav/src/propfind/contact_report_entry
  - functions/crates/lpe-dav/src/report/contact_matches_report
---

# Signature

`pub(crate) fn contact_href(collection_id: &str, id: Uuid) -> String`

# Called by

- [contact_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_resource_entry.md)
- [contact_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_report_entry.md)
- [contact_matches_report](../../../../../functions/crates/lpe-dav/src/report/contact_matches_report.md)