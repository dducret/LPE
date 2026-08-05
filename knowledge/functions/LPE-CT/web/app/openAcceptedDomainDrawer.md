---
type: JavaScript Function
title: openAcceptedDomainDrawer
resource: LPE-CT/web/app.js#L211-L281
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/findAcceptedDomain
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/app/acceptedDomainPayloadFromForm
  - functions/LPE-CT/web/app/validateAcceptedDomainPayload
  - functions/LPE-CT/web/modules/app/api/putJson
  - functions/LPE-CT/web/modules/app/api/postJson
  - functions/LPE-CT/web/app/currentAcceptedDomains
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/modules/app/system/renderPlatform
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openAcceptedDomainDrawer(domainId = null, opener = document.activeElement)`

# Calls

- [findAcceptedDomain](../../../../functions/LPE-CT/web/app/findAcceptedDomain.md)
- [showFeedback](../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)
- [renderDrawerForm](../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [escapeHtml](../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [acceptedDomainPayloadFromForm](../../../../functions/LPE-CT/web/app/acceptedDomainPayloadFromForm.md)
- [validateAcceptedDomainPayload](../../../../functions/LPE-CT/web/app/validateAcceptedDomainPayload.md)
- [putJson](../../../../functions/LPE-CT/web/modules/app/api/putJson.md)
- [postJson](../../../../functions/LPE-CT/web/modules/app/api/postJson.md)
- [currentAcceptedDomains](../../../../functions/LPE-CT/web/app/currentAcceptedDomains.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [renderPlatform](../../../../functions/LPE-CT/web/modules/app/system/renderPlatform.md)
- [closeDrawer](../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)

# Called by

- [getActionHandlers](../../../../functions/LPE-CT/web/app/getActionHandlers.md)