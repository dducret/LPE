---
type: TypeScript Function
title: cloneValue
resource: web/shared/src/i18n.ts#L80-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/shared/src/i18n/isPlainObject
  - functions/LPE-CT/web/app/smoke/test/MockFormData/entries
  called_by:
  - functions/web/shared/src/i18n/defineLocaleCatalog
  - functions/web/shared/src/i18n/mergeLocale
---

# Signature

`function cloneValue<T>(value: T): T`

# Calls

- [isPlainObject](../../../../../functions/web/shared/src/i18n/isPlainObject.md)
- [entries](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/entries.md)

# Called by

- [defineLocaleCatalog](../../../../../functions/web/shared/src/i18n/defineLocaleCatalog.md)
- [mergeLocale](../../../../../functions/web/shared/src/i18n/mergeLocale.md)