---
type: Python Module
title: mapi
resource: tools/rca_outlook/mapi.py#L1-L298
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/struct
  - external/uuid
  - external/urllib-parse
  - external/xml-etree-elementtree-as-et
  - external/dataclasses
  - external/itertools
  - external/http
---

# Contains

- [MapiHttpEndpoints](../../../classes/tools/rca_outlook/mapi/MapiHttpEndpoints.md)
- [xml_local_name](../../../functions/tools/rca_outlook/mapi/xml_local_name.md)
- [xml_child_text](../../../functions/tools/rca_outlook/mapi/xml_child_text.md)
- [parse_pox_mapi_http_endpoints](../../../functions/tools/rca_outlook/mapi/parse_pox_mapi_http_endpoints.md)
- [require_published_mapi_url](../../../functions/tools/rca_outlook/mapi/require_published_mapi_url.md)
- [mapi_session_cookie_state](../../../functions/tools/rca_outlook/mapi/mapi_session_cookie_state.md)
- [mapi_request_id](../../../functions/tools/rca_outlook/mapi/mapi_request_id.md)
- [mapi_client_info](../../../functions/tools/rca_outlook/mapi/mapi_client_info.md)
- [utf16z](../../../functions/tools/rca_outlook/mapi/utf16z.md)
- [contains_bytes](../../../functions/tools/rca_outlook/mapi/contains_bytes.md)
- [mapi_folder_id](../../../functions/tools/rca_outlook/mapi/mapi_folder_id.md)
- [mapi_wire_folder_id](../../../functions/tools/rca_outlook/mapi/mapi_wire_folder_id.md)
- [mapi_execute_body](../../../functions/tools/rca_outlook/mapi/mapi_execute_body.md)
- [mapi_rop_buffer](../../../functions/tools/rca_outlook/mapi/mapi_rop_buffer.md)
- [mapi_sent_subject_table_rops](../../../functions/tools/rca_outlook/mapi/mapi_sent_subject_table_rops.md)
- [mapi_sent_content_sync_rops](../../../functions/tools/rca_outlook/mapi/mapi_sent_content_sync_rops.md)
- [mapi_empty_deleted_items_rops](../../../functions/tools/rca_outlook/mapi/mapi_empty_deleted_items_rops.md)
- [resolve_names_request](../../../functions/tools/rca_outlook/mapi/resolve_names_request.md)
- [rpc_rts_conn_a1_body](../../../functions/tools/rca_outlook/mapi/rpc_rts_conn_a1_body.md)
- [rpc_rts_conn_b1_body](../../../functions/tools/rca_outlook/mapi/rpc_rts_conn_b1_body.md)
- [mapi_http_binary_payload](../../../functions/tools/rca_outlook/mapi/mapi_http_binary_payload.md)
- [mapi_execute_response_rops](../../../functions/tools/rca_outlook/mapi/mapi_execute_response_rops.md)
- [le_u32](../../../functions/tools/rca_outlook/mapi/le_u32.md)
- [nspi_first_minimal_id](../../../functions/tools/rca_outlook/mapi/nspi_first_minimal_id.md)
- [nspi_get_props_request](../../../functions/tools/rca_outlook/mapi/nspi_get_props_request.md)
- [assert_nspi_common_success](../../../functions/tools/rca_outlook/mapi/assert_nspi_common_success.md)
- [assert_nspi_resolve_names_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_resolve_names_payload.md)
- [assert_nspi_get_matches_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_get_matches_payload.md)
- [assert_nspi_query_rows_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_query_rows_payload.md)
- [assert_nspi_get_props_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_get_props_payload.md)
- [assert_nspi_fixture_payload](../../../functions/tools/rca_outlook/mapi/assert_nspi_fixture_payload.md)

# Imports

- `struct`
- `uuid`
- `urllib.parse`
- `xml.etree.ElementTree as ET`
- `dataclasses`
- `itertools`
- `.http`