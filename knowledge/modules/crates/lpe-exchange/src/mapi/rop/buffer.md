---
type: Rust Module
title: buffer
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L1-L265
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [Cursor](../../../../../../classes/crates/lpe-exchange/src/mapi/rop/buffer/Cursor.md)
- [new](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/new.md)
- [read_u32](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u32.md)
- [read_i32](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32.md)
- [read_i64](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i64.md)
- [read_u16](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [read_u8](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [read_ascii_z](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z.md)
- [read_utf16z](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_utf16z.md)
- [remaining](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [remaining_is_zero_padding](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining_is_zero_padding.md)
- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [write_u32](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u32.md)
- [write_u16](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [write_u16_prefixed_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [read_u16_prefixed_string](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/read_u16_prefixed_string.md)
- [write_u64](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_object_id.md)
- [write_utf16z](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_typed_string](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string.md)
- [write_typed_string_reduced_unicode_when_lossless](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string_reduced_unicode_when_lossless.md)
- [split_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_buffer.md)
- [split_rop_payload_best_effort](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_best_effort.md)
- [split_rop_payload_spec](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_spec.md)
- [split_rop_payload_legacy](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_legacy.md)
- [is_rpc_header_ext_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer.md)
- [rpc_header_ext_payload](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_payload.md)
- [rpc_header_ext_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)
- [rpc_header_ext_rop_buffer_with_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer_with_flags.md)
- [rpc_header_ext_rop_buffer_chain](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer_chain.md)

# Imports

- `anyhow::{anyhow, Result}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)