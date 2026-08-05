---
type: Rust Function
title: find_people_query_text
resource: crates/lpe-exchange/src/service/ews/directory.rs#L371-L380
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_text
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/directory/find_people_response
---

# Signature

`fn find_people_query_text(request: &str) -> String`

# Calls

- [element_text](../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_text.md)

# Called by

- [find_people_response](../../../../../../../functions/crates/lpe-exchange/src/service/ews/directory/find_people_response.md)