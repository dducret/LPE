---
type: Rust Function
title: xml_attribute_value
resource: crates/lpe-dav/src/report.rs#L69-L81
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/report/parse_report_filter
---

# Signature

`fn xml_attribute_value(xml: &str, element: &str, attribute: &str) -> Option<String>`

# Called by

- [parse_report_filter](../../../../../functions/crates/lpe-dav/src/report/parse_report_filter.md)