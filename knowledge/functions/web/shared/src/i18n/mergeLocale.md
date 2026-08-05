---
type: TypeScript Function
title: mergeLocale
resource: web/shared/src/i18n.ts#L60-L78
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/shared/src/i18n/cloneValue
  - functions/web/shared/src/i18n/isPlainObject
  called_by:
  - functions/web/shared/src/i18n/defineLocaleCatalog
---

# Signature

`function mergeLocale<T>(base: T, override: DeepPartial<T>): T`

# Calls

- [cloneValue](../../../../../functions/web/shared/src/i18n/cloneValue.md)
- [isPlainObject](../../../../../functions/web/shared/src/i18n/isPlainObject.md)

# Called by

- [defineLocaleCatalog](../../../../../functions/web/shared/src/i18n/defineLocaleCatalog.md)