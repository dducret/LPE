---
type: Rust Function
title: search_status_response
resource: crates/lpe-activesync/src/service.rs#L1456-L1471
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search
---

# Signature

`fn search_status_response( protocol_version: &str, search_status: &str, store_status: Option<&str>, ) -> Result<Response>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_search](../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)