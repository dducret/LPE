---
type: Rust Module
title: xml
resource: crates/lpe-exchange/src/service/ews/xml.rs#L1-L312
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/axum-http-header-content-type-headermap-headervalue-statuscode-response-intoresponse-response
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [xml_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/xml_response.md)
- [soap_response](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/soap_response.md)
- [decode_ews_body](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/decode_ews_body.md)
- [decode_utf16_body](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/decode_utf16_body.md)
- [operation_name](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/operation_name.md)
- [escape_xml](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/escape_xml.md)
- [attribute_values_for_tag](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_values_for_tag.md)
- [attribute_value_after](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)
- [ews_bool_attribute](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/ews_bool_attribute.md)
- [ews_usize_attribute](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/ews_usize_attribute.md)
- [count_folder_elements](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/count_folder_elements.md)
- [count_tag_occurrences](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/count_tag_occurrences.md)
- [attribute_value](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value.md)
- [open_tag_text](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/open_tag_text.md)
- [element_text](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)
- [element_content](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [element_contents](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_contents.md)
- [xml_text](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/xml_text.md)
- [html_to_text](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/html_to_text.md)
- [parse_xml_bool](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool.md)
- [parse_xml_bool_attr](../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/parse_xml_bool_attr.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `axum::{
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)