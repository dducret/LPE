---
type: Rust Function
title: parse_report_filter
resource: crates/lpe-dav/src/report.rs#L17-L28
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/report/xml_tag_values
  - functions/crates/lpe-dav/src/report/xml_text_match_values
  - functions/crates/lpe-dav/src/report/xml_attribute_value
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_report
---

# Signature

`pub(crate) fn parse_report_filter(body: &[u8]) -> Result<ReportFilter>`

# Calls

- [xml_tag_values](../../../../../functions/crates/lpe-dav/src/report/xml_tag_values.md)
- [xml_text_match_values](../../../../../functions/crates/lpe-dav/src/report/xml_text_match_values.md)
- [xml_attribute_value](../../../../../functions/crates/lpe-dav/src/report/xml_attribute_value.md)

# Called by

- [handle_report](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)