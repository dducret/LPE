import { LOCALE_KEY, baseMessages, localeLabels, localizedMessages, supportedLocales } from "./messages.js?v=20260501-system-diagnostic-pending";

const { createI18n, defineLocaleCatalog } = window.LpeCtI18n;

const messages = defineLocaleCatalog({
  supportedLocales,
  defaultLocale: "en",
  base: baseMessages,
  localized: localizedMessages,
});

export const i18n = createI18n({
  storageKey: LOCALE_KEY,
  supportedLocales,
  localeLabels,
  messages,
});

export function getCopy() {
  return i18n.getCopy();
}

export function translate(template, values = {}) {
  return i18n.translate(template, values);
}
