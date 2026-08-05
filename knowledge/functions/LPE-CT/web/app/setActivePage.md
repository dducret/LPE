---
type: JavaScript Function
title: setActivePage
resource: LPE-CT/web/app.js#L57-L72
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/modules/pages/activatePageView
  - functions/LPE-CT/web/app/syncPageTabs
  - functions/LPE-CT/web/app/smoke/test/MockElement/focus
  called_by:
  - functions/LPE-CT/web/app/updateNavState
  - functions/LPE-CT/web/app/registerSectionObserver
  - functions/LPE-CT/web/app/handleBodyClick
---

# Signature

`function setActivePage(page = state.activePage, options = {})`

# Calls

- [activatePageView](../../../../functions/LPE-CT/web/modules/pages/activatePageView.md)
- [syncPageTabs](../../../../functions/LPE-CT/web/app/syncPageTabs.md)
- [focus](../../../../functions/LPE-CT/web/app/smoke/test/MockElement/focus.md)

# Called by

- [updateNavState](../../../../functions/LPE-CT/web/app/updateNavState.md)
- [registerSectionObserver](../../../../functions/LPE-CT/web/app/registerSectionObserver.md)
- [handleBodyClick](../../../../functions/LPE-CT/web/app/handleBodyClick.md)