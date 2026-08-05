---
type: JavaScript Function
title: saveReporting
resource: LPE-CT/web/app.js#L138-L144
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/app/api/putJson
  - functions/LPE-CT/web/app/renderDashboard
  called_by:
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestSettingsDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestDefault
  - functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer
  - functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestOverride
---

# Signature

`async function saveReporting(settings)`

# Calls

- [putJson](../../../../functions/LPE-CT/web/modules/app/api/putJson.md)
- [renderDashboard](../../../../functions/LPE-CT/web/app/renderDashboard.md)

# Called by

- [openDigestSettingsDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestSettingsDrawer.md)
- [openDigestDefaultDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestDefaultDrawer.md)
- [deleteDigestDefault](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestDefault.md)
- [openDigestOverrideDrawer](../../../../functions/LPE-CT/web/modules/app/policy-drawers/openDigestOverrideDrawer.md)
- [deleteDigestOverride](../../../../functions/LPE-CT/web/modules/app/policy-drawers/deleteDigestOverride.md)