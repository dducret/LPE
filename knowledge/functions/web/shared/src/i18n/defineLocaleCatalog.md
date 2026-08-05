---
type: TypeScript Function
title: defineLocaleCatalog
resource: web/shared/src/i18n.ts#L47-L58
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/web/shared/src/i18n/cloneValue
  - functions/web/shared/src/i18n/mergeLocale
---

# Signature

`function defineLocaleCatalog<T extends Record<string, unknown>>( base: T, localized: Record<Exclude<Locale, "en">, DeepPartial<T>>, ): LocaleCatalog<T>`

# Calls

- [cloneValue](../../../../../functions/web/shared/src/i18n/cloneValue.md)
- [mergeLocale](../../../../../functions/web/shared/src/i18n/mergeLocale.md)