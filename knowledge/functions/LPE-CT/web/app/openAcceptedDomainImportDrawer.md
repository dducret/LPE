---
type: JavaScript Function
title: openAcceptedDomainImportDrawer
resource: LPE-CT/web/app.js#L283-L342
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/policy-drawers/parseLines
  - functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/validateAcceptedDomainPayload
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/modules/app/system/renderPlatform
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openAcceptedDomainImportDrawer(opener = document.activeElement)`

# Calls

- [renderDrawerForm](../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [escapeHtml](../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [parseLines](../../../../functions/LPE-CT/web/modules/app/policy-drawers/parseLines.md)
- [normalizeDomain](../../../../functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [validateAcceptedDomainPayload](../../../../functions/LPE-CT/web/app/validateAcceptedDomainPayload.md)
- [postJson](../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [renderPlatform](../../../../functions/LPE-CT/web/modules/app/system/renderPlatform.md)
- [closeDrawer](../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [showFeedback](../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../functions/LPE-CT/web/app/getActionHandlers.md)