---
type: Python Function
title: is_source_file
resource: tools/check_oversized_sources.py#L129-L142
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/check_oversized_sources/is_generated
  - functions/tools/check_oversized_sources/is_test_path
  called_by:
  - functions/tools/check_oversized_sources/main
---

# Signature

`def is_source_file(root: Path, path: Path, include_tests: bool) -> bool:`

# Calls

- [is_generated](../../../functions/tools/check_oversized_sources/is_generated.md)
- [is_test_path](../../../functions/tools/check_oversized_sources/is_test_path.md)

# Called by

- [main](../../../functions/tools/check_oversized_sources/main.md)