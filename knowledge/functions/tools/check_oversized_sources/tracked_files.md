---
type: Python Function
title: tracked_files
resource: tools/check_oversized_sources.py#L94-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/check_oversized_sources/walked_files
  called_by:
  - functions/tools/check_oversized_sources/main
---

# Signature

`def tracked_files(root: Path) -> list[Path]:`

# Calls

- [walked_files](../../../functions/tools/check_oversized_sources/walked_files.md)

# Called by

- [main](../../../functions/tools/check_oversized_sources/main.md)