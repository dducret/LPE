---
type: JavaScript Function
title: getPlatformDrawerConfigs
resource: LPE-CT/web/app.js#L364-L489
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/app/smoke/test/MockFormData/entries
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/modules/app/format/isValidHostname
  - functions/LPE-CT/web/modules/app/policy-drawers/parseLines
  - functions/LPE-CT/web/modules/app/system/publicTlsSettings
  called_by:
  - functions/LPE-CT/web/app/openPlatformDrawer
---

# Signature

`function getPlatformDrawerConfigs(dashboard, copy)`

# Calls

- [escapeHtml](../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [entries](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/entries.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [isValidHostname](../../../../functions/LPE-CT/web/modules/app/format/isValidHostname.md)
- [parseLines](../../../../functions/LPE-CT/web/modules/app/policy-drawers/parseLines.md)
- [publicTlsSettings](../../../../functions/LPE-CT/web/modules/app/system/publicTlsSettings.md)

# Called by

- [openPlatformDrawer](../../../../functions/LPE-CT/web/app/openPlatformDrawer.md)