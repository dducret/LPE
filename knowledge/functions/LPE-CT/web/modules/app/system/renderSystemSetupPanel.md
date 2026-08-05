---
type: JavaScript Function
title: renderSystemSetupPanel
resource: LPE-CT/web/modules/app/system.js#L206-L219
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

`function renderSystemSetupPanel(title, summary, body, actions = "")`

# Calls

- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)

# Called by

- [renderNetworkSetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderNetworkSetup.md)
- [renderMailRelaySetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderMailRelaySetup.md)
- [renderMailAuthenticationSetup](../../../../../../functions/LPE-CT/web/modules/app/system/renderMailAuthenticationSetup.md)
- [renderPlatform](../../../../../../functions/LPE-CT/web/modules/app/system/renderPlatform.md)