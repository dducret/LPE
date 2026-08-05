---
type: Rust Module
title: parse
resource: crates/lpe-managesieve/src/parse.rs#L1-L153
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-bail-result
  - external/tokio-io-asyncbufreadext-asyncreadext
  member_of:
  - packages/crates/lpe-managesieve
---

# Contains

- [Request](../../../../classes/crates/lpe-managesieve/src/parse/Request.md)
- [Argument](../../../../classes/crates/lpe-managesieve/src/parse/Argument.md)
- [single_string_arg](../../../../functions/crates/lpe-managesieve/src/parse/single_string_arg.md)
- [as_string](../../../../functions/crates/lpe-managesieve/src/parse/as_string.md)
- [read_request](../../../../functions/crates/lpe-managesieve/src/parse/read_request.md)
- [parse_request_line](../../../../functions/crates/lpe-managesieve/src/parse/parse_request_line.md)
- [parse_atom](../../../../functions/crates/lpe-managesieve/src/parse/parse_atom.md)
- [parse_quoted](../../../../functions/crates/lpe-managesieve/src/parse/parse_quoted.md)
- [parse_literal_marker](../../../../functions/crates/lpe-managesieve/src/parse/parse_literal_marker.md)
- [skip_ws](../../../../functions/crates/lpe-managesieve/src/parse/skip_ws.md)

# Imports

- `anyhow::{bail, Result}`
- `tokio::io::{AsyncBufReadExt, AsyncReadExt}`

# Member of

- [lpe-managesieve](../../../../packages/crates/lpe-managesieve.md)