# Stalwart Reference Constraint

## Current State/Functionality Overview

Stalwart is a product and architecture benchmark only. `LPE` must not copy Stalwart code or adopt licensing that conflicts with `LICENSE.md`.

## Implementation/Usage

- Stalwart code must not be reused in `LPE`.
- `LPE` source code remains `Apache-2.0`.
- `MIT` dependencies require the exception policy in `LICENSE.md`.
- `AGPL`, `LGPL`, `GPL`, `SSPL`, and non-standard licenses are forbidden.
- Stalwart may inform product benchmarking only.
- Architecture decisions must remain aligned with the `LPE` split:
  - core `LPE` owns canonical mailbox and collaboration state
  - `LPE-CT` owns SMTP edge, relay, quarantine, and perimeter enforcement
- Do not replace the split architecture with an all-in-one SMTP/core design.
- Do not add protocol breadth ahead of documented completion depth.

## Reference Table/List

| Benchmark area | `LPE` rule |
| --- | --- |
| licensing | follow `LICENSE.md` |
| edge mail | keep in `LPE-CT` |
| canonical mailbox state | keep in core `LPE` |
| transport filtering | keep in `LPE-CT` |
| dependencies | review before adoption |
| implementation ideas | re-design for `LPE`; do not copy code |
