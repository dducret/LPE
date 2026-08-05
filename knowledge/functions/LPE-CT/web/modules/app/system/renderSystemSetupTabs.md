---
type: JavaScript Function
title: renderSystemSetupTabs
resource: LPE-CT/web/modules/app/system.js#L183-L204
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  called_by:
  - functions/LPE-CT/web/modules/app/system/renderNetworkSetup
  - functions/LPE-CT/web/modules/app/system/renderMailRelaySetup
  - functions/LPE-CT/web/modules/app/system/renderMailAuthenticationSetup
  - functions/LPE-CT/web/modules/app/system/renderPlatform
---

# Signature

`function renderSystemSetupTabs(tabs, activeTab, level = "primary")`

# Calls

- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)

# Called by

- [renderNetworkSetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderNetworkSetup.md)
- [renderMailRelaySetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderMailRelaySetup.md)
- [renderMailAuthenticationSetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderMailAuthenticationSetup.md)
- [renderPlatform](../../../../../../functions/LPE-CT/web/modules/app/system/renderPlatform.md)