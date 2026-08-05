---
type: JavaScript Function
title: openDkimDomainDrawer
resource: LPE-CT/web/modules/app/policy-drawers.js#L514-L576
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/currentPolicies
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidDomain
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidSelector
  - functions/LPE-CT/web/app/savePolicies
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openDkimDomainDrawer(index = null, opener = document.activeElement)`

# Calls

- [currentPolicies](../../../../../../functions/LPE-CT/web/modules/app/format/currentPolicies.md)
- [renderDrawerForm](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [normalizeDomain](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain.md)
- [isValidDomain](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidDomain.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [isValidSelector](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidSelector.md)
- [savePolicies](../../../../../../functions/LPE-CT/web/app/savePolicies.md)
- [closeDrawer](../../../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)