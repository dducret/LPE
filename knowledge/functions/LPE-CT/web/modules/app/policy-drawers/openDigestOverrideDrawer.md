---
type: JavaScript Function
title: openDigestOverrideDrawer
resource: LPE-CT/web/modules/app/policy-drawers.js#L705-L761
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/format/currentReporting
  - functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm
  - functions/LPE-CT/web/modules/app/format/escapeHtml
  - functions/LPE-CT/web/modules/app/policy-drawers/normalizeEmail
  - functions/LPE-CT/web/modules/app/policy-drawers/isValidEmail
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/LPE-CT/web/app/saveReporting
  - functions/LPE-CT/web/modules/app/ui/closeDrawer
  - functions/LPE-CT/web/modules/app/ui/showFeedback
  called_by:
  - functions/LPE-CT/web/app/getActionHandlers
---

# Signature

`function openDigestOverrideDrawer(index = null, opener = document.activeElement)`

# Calls

- [currentReporting](../../../../../../functions/LPE-CT/web/modules/app/format/currentReporting.md)
- [renderDrawerForm](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/renderDrawerForm.md)
- [escapeHtml](../../../../../../functions/LPE-CT/web/modules/app/format/escapeHtml.md)
- [normalizeEmail](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/normalizeEmail.md)
- [isValidEmail](../../../../../../functions/LPE-CT/web/modules/app/policy-drawers/isValidEmail.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [saveReporting](../../../../../../functions/LPE-CT/web/app/saveReporting.md)
- [closeDrawer](../../../../../../functions/LPE-CT/web/modules/app/ui/closeDrawer.md)
- [showFeedback](../../../../../../functions/LPE-CT/web/modules/app/ui/showFeedback.md)

# Called by

- [getActionHandlers](../../../../../../functions/LPE-CT/web/app/getActionHandlers.md)