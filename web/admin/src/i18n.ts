export type Locale = "en" | "fr" | "de" | "it" | "es";

export const supportedLocales: Locale[] = ["en", "fr", "de", "it", "es"];

export const localeLabels: Record<Locale, string> = {
  en: "English",
  fr: "Francais",
  de: "Deutsch",
  it: "Italiano",
  es: "Espanol"
};

export const messages = {
  en: {
    eyebrow: "La Poste ELectronique",
    title: "Administration Console",
    body: "Initial back-office entry point to manage accounts, domains, quotas, and policies.",
    languageLabel: "Language"
  },
  fr: {
    eyebrow: "La Poste ELectronique",
    title: "Console d'administration",
    body: "Point d'entree initial du back office pour piloter comptes, domaines, quotas et politiques.",
    languageLabel: "Langue"
  },
  de: {
    eyebrow: "La Poste ELectronique",
    title: "Administrationskonsole",
    body: "Erster Backoffice-Einstiegspunkt zur Verwaltung von Konten, Domains, Quoten und Richtlinien.",
    languageLabel: "Sprache"
  },
  it: {
    eyebrow: "La Poste ELectronique",
    title: "Console di amministrazione",
    body: "Punto di accesso iniziale del back office per gestire account, domini, quote e criteri.",
    languageLabel: "Lingua"
  },
  es: {
    eyebrow: "La Poste ELectronique",
    title: "Consola de administracion",
    body: "Punto de entrada inicial del back office para gestionar cuentas, dominios, cuotas y politicas.",
    languageLabel: "Idioma"
  }
} satisfies Record<
  Locale,
  {
    eyebrow: string;
    title: string;
    body: string;
    languageLabel: string;
  }
>;

export function getInitialLocale(): Locale {
  if (typeof window === "undefined") {
    return "en";
  }

  const stored = window.localStorage.getItem("lpe.locale");
  if (stored && supportedLocales.includes(stored as Locale)) {
    return stored as Locale;
  }

  return "en";
}
