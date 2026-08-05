---
type: Python Function
title: autodiscover_url
resource: tools/activesync_mobile_lab_preflight.py#L49-L50
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/activesync_mobile_lab_preflight/check_autodiscover_json
  - functions/tools/activesync_mobile_lab_preflight/check_desktop_pox
  - functions/tools/activesync_mobile_lab_preflight/check_mobilesync_pox
---

# Signature

`def autodiscover_url(base_url: str, path: str) -> str:`

# Called by

- [check_autodiscover_json](../../../functions/tools/activesync_mobile_lab_preflight/check_autodiscover_json.md)
- [check_desktop_pox](../../../functions/tools/activesync_mobile_lab_preflight/check_desktop_pox.md)
- [check_mobilesync_pox](../../../functions/tools/activesync_mobile_lab_preflight/check_mobilesync_pox.md)