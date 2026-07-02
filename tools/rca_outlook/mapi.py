from __future__ import annotations

import struct
import uuid
from itertools import count

from .http import require


MAPI_RCA_REQUEST_GUID = uuid.uuid4()
MAPI_RCA_CLIENT_GUID = uuid.uuid4()
MAPI_RCA_COUNTER = count(1)

def mapi_request_id(sequence: str | int | None = None) -> str:
    value = next(MAPI_RCA_COUNTER) if sequence is None else sequence
    if isinstance(value, str) and not value.isdecimal():
        value = next(MAPI_RCA_COUNTER)
    return f"{{{str(MAPI_RCA_REQUEST_GUID).upper()}}}:{value}"

def mapi_client_info() -> str:
    return f"{{{str(MAPI_RCA_CLIENT_GUID).upper()}}}:{next(MAPI_RCA_COUNTER)}"

def utf16z(value: str) -> bytes:
    return value.encode("utf-16le") + b"\x00\x00"

def contains_bytes(haystack: bytes, needle: bytes) -> bool:
    return any(haystack[index : index + len(needle)] == needle for index in range(0, len(haystack) - len(needle) + 1))

def mapi_folder_id(global_counter: int) -> int:
    return ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | 1

def mapi_execute_body(rop_buffer: bytes) -> bytes:
    body = bytearray()
    body.extend(struct.pack("<I", 0))
    body.extend(struct.pack("<I", len(rop_buffer)))
    body.extend(rop_buffer)
    body.extend(struct.pack("<I", 4096))
    body.extend(struct.pack("<I", 0))
    return bytes(body)

def mapi_rop_buffer(rops: bytes, handles: list[int]) -> bytes:
    buffer = bytearray()
    buffer.extend(struct.pack("<H", len(rops)))
    buffer.extend(rops)
    for handle in handles:
        buffer.extend(struct.pack("<I", handle))
    return bytes(buffer)

def mapi_sent_subject_table_rops(row_count: int = 20) -> bytes:
    rops = bytearray()
    rops.extend([0x02, 0x00, 0x00, 0x01])  # RopOpenFolder
    rops.extend(struct.pack("<Q", mapi_folder_id(7)))  # canonical Sent
    rops.append(0)
    rops.extend([0x05, 0x00, 0x01, 0x02, 0x00])  # RopGetContentsTable
    rops.extend([0x12, 0x00, 0x02, 0x00])  # RopSetColumns
    rops.extend(struct.pack("<H", 1))
    rops.extend(struct.pack("<I", 0x0037_001F))  # PidTagSubject, Unicode
    rops.extend([0x15, 0x00, 0x02, 0x00, 0x01])  # RopQueryRows
    rops.extend(struct.pack("<H", row_count))
    return bytes(rops)

def mapi_sent_content_sync_rops(buffer_size: int = 4096) -> bytes:
    rops = bytearray()
    rops.extend([0x02, 0x00, 0x00, 0x01])  # RopOpenFolder
    rops.extend(struct.pack("<Q", mapi_folder_id(7)))  # canonical Sent
    rops.append(0)
    rops.extend([0x70, 0x00, 0x01, 0x02])  # RopSynchronizationConfigure
    rops.extend([0x01, 0x00])  # content sync, Unicode send option
    rops.extend(struct.pack("<H", 0))  # SynchronizationFlags
    rops.extend(struct.pack("<H", 0))  # RestrictionDataSize
    rops.extend(struct.pack("<I", 0))  # SynchronizationExtraFlags
    rops.extend(struct.pack("<H", 0))  # PropertyTagCount
    rops.extend([0x4E, 0x00, 0x02])  # RopFastTransferSourceGetBuffer
    rops.extend(struct.pack("<H", buffer_size))
    return bytes(rops)

def mapi_empty_deleted_items_rops() -> bytes:
    rops = bytearray()
    rops.extend([0x02, 0x00, 0x00, 0x01])  # RopOpenFolder
    rops.extend(struct.pack("<Q", mapi_folder_id(8)))  # canonical Deleted Items / Trash
    rops.append(0)
    rops.extend([0x58, 0x00, 0x01, 0x00, 0x00])  # RopEmptyFolder, no flags
    return bytes(rops)

def resolve_names_request(search_address: str, columns: list[int]) -> bytes:
    body = bytearray()
    body.extend(struct.pack("<I", 0))
    body.append(0xFF)
    body.extend(bytes(24))
    body.extend(struct.pack("<I", 1252))
    body.extend(struct.pack("<I", 0x0409))
    body.extend(struct.pack("<I", 0x0409))
    body.append(0xFF)
    body.extend(struct.pack("<I", len(columns)))
    for column in columns:
        body.extend(struct.pack("<I", column))
    body.append(0xFF)
    body.extend(struct.pack("<I", 1))
    unresolved_name = utf16z(f"=SMTP:{search_address}")
    body.extend(struct.pack("<H", len(unresolved_name)))
    body.extend(unresolved_name)
    body.extend(struct.pack("<I", 0))
    return bytes(body)

def rpc_rts_conn_a1_body(receive_window_size: int = 0x00010000) -> bytes:
    virtual_connection_cookie = uuid.uuid4().bytes_le
    out_channel_cookie = uuid.uuid4().bytes_le
    body = bytearray()
    body.extend(b"\x05\x00\x14\x03\x10\x00\x00\x00")
    body.extend(struct.pack("<HHIHH", 76, 0, 0, 0, 4))
    body.extend(struct.pack("<II", 6, 1))
    body.extend(struct.pack("<I", 3))
    body.extend(virtual_connection_cookie)
    body.extend(struct.pack("<I", 3))
    body.extend(out_channel_cookie)
    body.extend(struct.pack("<II", 0, receive_window_size))
    return bytes(body)

def rpc_rts_conn_b1_body(virtual_connection_cookie: bytes) -> bytes:
    body = bytearray(bytes.fromhex(
        "0500140310000000680000000000000000000600"
        "06000000010000000300000076ed340685c5dd390e9a6acbc8cb9951"
        "03000000a6c4ac6df261ef9fc3804d0c73a59fff"
        "040000000000004005000000e09304000c0000005475b4942dd08746bf4c3d2821816b2c"
    ))
    body[32:48] = virtual_connection_cookie
    return bytes(body)

def mapi_http_binary_payload(body: bytes) -> bytes:
    _, separator, payload = body.partition(b"\r\n\r\n")
    require(separator == b"\r\n\r\n", "MAPI response body did not contain the header/body separator")
    return payload

def mapi_execute_response_rops(payload: bytes, label: str) -> bytes:
    require(len(payload) >= 16, f"{label} returned a truncated Execute response")
    require(le_u32(payload, 0) == 0, f"{label} returned nonzero Execute StatusCode")
    require(le_u32(payload, 4) == 0, f"{label} returned nonzero Execute ErrorCode")
    rop_buffer_size = le_u32(payload, 12)
    require(rop_buffer_size >= 2, f"{label} returned an empty ROP buffer")
    require(len(payload) >= 16 + rop_buffer_size, f"{label} returned a truncated ROP buffer")
    rop_buffer = payload[16 : 16 + rop_buffer_size]
    response_rop_size = struct.unpack_from("<H", rop_buffer, 0)[0]
    require(response_rop_size > 0, f"{label} returned no response ROPs")
    require(len(rop_buffer) >= 2 + response_rop_size, f"{label} returned truncated response ROPs")
    return rop_buffer[2 : 2 + response_rop_size]

def le_u32(payload: bytes, offset: int) -> int:
    require(len(payload) >= offset + 4, f"MAPI payload is too short for u32 at offset {offset}")
    return struct.unpack_from("<I", payload, offset)[0]

def nspi_first_minimal_id(payload: bytes, request_type: str) -> int:
    assert_nspi_get_matches_payload(payload, request_type)
    minimal_id = le_u32(payload, 14)
    require(minimal_id != 0, f"MAPI NSPI {request_type} returned a zero MinimalId")
    return minimal_id

def nspi_get_props_request(minimal_id: int, property_tags: list[int]) -> bytes:
    body = bytearray(28)
    struct.pack_into("<I", body, 12, minimal_id)
    for tag in property_tags:
        body.extend(struct.pack("<I", tag))
    return bytes(body)

def assert_nspi_common_success(payload: bytes, request_type: str) -> None:
    require(len(payload) >= 12, f"MAPI NSPI {request_type} returned a truncated success payload")
    require(le_u32(payload, 0) == 0, f"MAPI NSPI {request_type} returned nonzero StatusCode")
    require(le_u32(payload, 4) == 0, f"MAPI NSPI {request_type} returned nonzero ErrorCode")

def assert_nspi_resolve_names_payload(payload: bytes, request_type: str) -> None:
    assert_nspi_common_success(payload, request_type)
    require(le_u32(payload, 8) == 1200, "ResolveNames did not return Unicode CodePage 1200")
    require(payload[12] == 1, "ResolveNames omitted MinimalIds")
    require(le_u32(payload, 13) == 1, "ResolveNames returned an unexpected MinimalId count")
    require(payload[21] == 1, "ResolveNames omitted row columns")
    require(le_u32(payload, 22) >= 2, "ResolveNames returned too few property tags")

def assert_nspi_get_matches_payload(payload: bytes, request_type: str) -> None:
    assert_nspi_common_success(payload, request_type)
    require(payload[8] == 0, "GetMatches unexpectedly returned STAT")
    require(payload[9] == 1, "GetMatches omitted MinimalIds")
    require(le_u32(payload, 10) == 1, "GetMatches returned an unexpected MinimalId count")
    require(payload[18] == 1, "GetMatches omitted row columns")
    require(le_u32(payload, 19) >= 4, "GetMatches returned too few property tags")

def assert_nspi_query_rows_payload(payload: bytes, request_type: str) -> None:
    assert_nspi_common_success(payload, request_type)
    require(payload[8] == 0, "QueryRows unexpectedly returned STAT")
    require(payload[9] == 1, "QueryRows omitted row columns")
    require(le_u32(payload, 10) >= 4, "QueryRows returned too few property tags")

def assert_nspi_get_props_payload(payload: bytes, request_type: str) -> None:
    assert_nspi_common_success(payload, request_type)
    require(le_u32(payload, 8) == 1200, "GetProps did not return Unicode CodePage 1200")
    require(payload[12] == 1, "GetProps omitted property values")
    require(le_u32(payload, 13) == 0, "GetProps returned nonzero row status")
    require(le_u32(payload, 17) >= 4, "GetProps returned too few property values")

def assert_nspi_fixture_payload(payload: bytes, request_type: str, expected_name: str, expected_email: str) -> None:
    require(
        contains_bytes(payload, utf16z(expected_email)),
        f"MAPI NSPI {request_type} did not return fixture email {expected_email}",
    )
    require(
        contains_bytes(payload, utf16z(expected_name)),
        f"MAPI NSPI {request_type} did not return fixture display name {expected_name}",
    )
