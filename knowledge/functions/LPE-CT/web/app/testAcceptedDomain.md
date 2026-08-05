---
type: JavaScript Function
title: testAcceptedDomain
resource: LPE-CT/web/app.js#L352-L362
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/app/currentAcceptedDomains
  - functions/LPE-CT/web/modules/app/system/renderPlatform
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`async function testAcceptedDomain(domainId)`

# Calls

- [postJson](../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [currentAcceptedDomains](../../../../functions/LPE-CT/web/app/currentAcceptedDomains.md)
- [renderPlatform](../../../../functions/LPE-CT/web/modules/app/system/renderPlatform.md)
- [showFeedback](../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../functions/LPE-CT/web/app/getActionHandlers.md)