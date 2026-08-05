---
type: JavaScript Function
title: openDigestDefaultDrawer
resource: LPE-CT/web/modules/app/policy-drawers.js#L642-L695
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/currentReporting
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain
  - functions/LPE-CT/web/modules/app/format/dedupeList
  - functions/LPE-CT/web/modules/app/policy-drawers/parseLines
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidDomain
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidEmail
  - functions/LPE-CT/web/app/saveReporting
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openDigestDefaultDrawer(index = null, opener = document.activeElement)`

# Calls

- [currentReporting](../../../../../../functions/LPE-CT/web/modules/app/format/currentReporting.md)
- [renderDrawerForm](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [normalizeDomain](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/normalizeDomain.md)
- [dedupeList](../../../../../../functions/LPE-CT/web/modules/app/format/dedupeList.md)
- [parseLines](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/parseLines.md)
- [isValidDomain](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidDomain.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [isValidEmail](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidEmail.md)
- [saveReporting](../../../../../../functions/LPE-CT/web/app/saveReporting.md)
- [closeDrawer](../../../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)