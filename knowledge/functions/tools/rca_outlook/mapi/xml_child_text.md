---
type: Python Function
title: xml_child_text
resource: tools/rca_outlook/mapi.py#L29-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/mapi/xml_local_name
  called_by:
  - functions/tools/rca_outlook/mapi/parse_pox_mapi_http_endpoints
---

# Signature

`def xml_child_text(element: ET.Element, name: str) -> str:`

# Calls

- [xml_local_name](../../../../functions/tools/rca_outlook/mapi/xml_local_name.md)

# Called by

- [parse_pox_mapi_http_endpoints](../../../../functions/tools/rca_outlook/mapi/parse_pox_mapi_http_endpoints.md)