---
type: Rust Function
title: search_query_text
resource: crates/lpe-activesync/src/service/search.rs#L114-L129
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search
---

# Signature

`fn search_query_text(store: &WbxmlNode) -> Option<String>`

# Calls

- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [handle_search](../../../../../../functions/crates/lpe-activesync/src/service/search/ActiveSyncService/handle_search.md)