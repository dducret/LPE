---
type: Rust Method
title: handle_search
resource: crates/lpe-activesync/src/service/search.rs#L16-L111
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/search_status_response
  - functions/crates/lpe-activesync/src/service/search/search_query_text
  - functions/crates/lpe-activesync/src/service/search/parse_range
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/service/sync_helpers/value_to_wbxml
  - functions/crates/lpe-activesync/src/snapshot/email_application_data
  - functions/crates/lpe-activesync/src/service/search/trim_preview
  - functions/crates/lpe-activesync/src/response/wbxml_response
---

# Signature

`pub(super) async fn handle_search( &self, principal: &AuthenticatedPrincipal, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [search_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/search_status_response.md)
- [search_query_text](../../../../../../../functions/crates/lpe-activesync/src/service/search/search_query_text.md)
- [parse_range](../../../../../../../functions/crates/lpe-activesync/src/service/search/parse_range.md)
- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [value_to_wbxml](../../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/value_to_wbxml.md)
- [email_application_data](../../../../../../../functions/crates/lpe-activesync/src/snapshot/email_application_data.md)
- [trim_preview](../../../../../../../functions/crates/lpe-activesync/src/service/search/trim_preview.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)