---
type: Rust Function
title: text_unescape
resource: crates/lpe-dav/src/parse.rs#L339-L346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_vcard
  - functions/crates/lpe-dav/src/parse/parse_ical
  - functions/crates/lpe-dav/src/parse/parse_vtodo
  - functions/crates/lpe-dav/src/parse/property_parameter
---

# Signature

`fn text_unescape(value: &str) -> String`

# Called by

- [parse_vcard](../../../../../functions/crates/lpe-dav/src/parse/parse_vcard.md)
- [parse_ical](../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [parse_vtodo](../../../../../functions/crates/lpe-dav/src/parse/parse_vtodo.md)
- [property_parameter](../../../../../functions/crates/lpe-dav/src/parse/property_parameter.md)