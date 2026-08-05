---
type: Rust Function
title: xml_tag_values
resource: crates/lpe-dav/src/report.rs#L30-L47
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-dav/src/report/parse_report_filter
---

# Signature

`fn xml_tag_values(xml: &str, local_name: &str) -> Vec<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_report_filter](../../../../../functions/crates/lpe-dav/src/report/parse_report_filter.md)