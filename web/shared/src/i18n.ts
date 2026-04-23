export type Locale = "en" | "fr" | "de" | "it" | "es";

export const supportedLocales: Locale[] = ["en", "fr", "de", "it", "es"];
export const localeLabels: Record<Locale, string> = {
  en: "English",
  fr: "Francais",
  de: "Deutsch",
  it: "Italiano",
  es: "Espanol",
};

const DEFAULT_LOCALE: Locale = "en";
const LOCALE_STORAGE_KEY = "lpe.locale";

type Primitive = string | number | boolean | null | undefined | symbol | bigint;
type DeepPartial<T> =
  T extends Primitive ? T :
  T extends readonly (infer U)[] ? readonly DeepPartial<U>[] :
  T extends (infer U)[] ? DeepPartial<U>[] :
  { [K in keyof T]?: DeepPartial<T[K]> };

export type LocaleCatalog<T> = Record<Locale, Readonly<T>>;

export function isLocale(value: string | null | undefined): value is Locale {
  return value != null && supportedLocales.includes(value as Locale);
}

export function getInitialLocale(): Locale {
  if (typeof window === "undefined") return DEFAULT_LOCALE;

  const stored = window.localStorage.getItem(LOCALE_STORAGE_KEY);
  if (isLocale(stored)) return stored;

  for (const language of window.navigator.languages) {
    const normalized = language.toLowerCase().split("-")[0];
    if (isLocale(normalized)) return normalized;
  }

  return DEFAULT_LOCALE;
}

export function setStoredLocale(locale: Locale) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(LOCALE_STORAGE_KEY, locale);
}

export function defineLocaleCatalog<T extends Record<string, unknown>>(
  base: T,
  localized: Record<Exclude<Locale, "en">, DeepPartial<T>>,
): LocaleCatalog<T> {
  return {
    en: deepFreeze(cloneValue(base)),
    fr: deepFreeze(mergeLocale(base, localized.fr)),
    de: deepFreeze(mergeLocale(base, localized.de)),
    it: deepFreeze(mergeLocale(base, localized.it)),
    es: deepFreeze(mergeLocale(base, localized.es)),
  };
}

function mergeLocale<T>(base: T, override: DeepPartial<T>): T {
  if (override === undefined) return cloneValue(base);
  if (Array.isArray(base)) {
    return cloneValue((override as T | undefined) ?? base);
  }
  if (!isPlainObject(base) || !isPlainObject(override)) {
    return cloneValue((override as T | undefined) ?? base);
  }

  const output: Record<string, unknown> = {};
  const baseRecord = base as Record<string, unknown>;
  const overrideRecord = override as Record<string, unknown>;
  for (const key of Object.keys(baseRecord)) {
    output[key] = key in overrideRecord
      ? mergeLocale(baseRecord[key], overrideRecord[key] as DeepPartial<unknown>)
      : cloneValue(baseRecord[key]);
  }
  return output as T;
}

function cloneValue<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((entry) => cloneValue(entry)) as T;
  }
  if (isPlainObject(value)) {
    const output: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      output[key] = cloneValue(entry);
    }
    return output as T;
  }
  return value;
}

function deepFreeze<T>(value: T): Readonly<T> {
  if (Array.isArray(value)) {
    for (const entry of value) {
      deepFreeze(entry);
    }
    return Object.freeze(value);
  }
  if (isPlainObject(value)) {
    for (const entry of Object.values(value)) {
      deepFreeze(entry);
    }
    return Object.freeze(value);
  }
  return value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
