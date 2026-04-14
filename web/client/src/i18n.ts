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
    navInbox: "Inbox",
    navCalendar: "Calendar",
    navContacts: "Contacts",
    eyebrow: "Web Client",
    title: "Modern JMAP-oriented interface",
    cardTitle: "Welcome to LPE",
    cardBody: "This web client is the foundation for mail, calendars, and contacts through a modern API.",
    languageLabel: "Language"
  },
  fr: {
    navInbox: "Boite de reception",
    navCalendar: "Calendrier",
    navContacts: "Contacts",
    eyebrow: "Client web",
    title: "Interface moderne orientee JMAP",
    cardTitle: "Bienvenue dans LPE",
    cardBody: "Ce client web sert de base pour la consultation mail, les calendriers et les contacts via une API moderne.",
    languageLabel: "Langue"
  },
  de: {
    navInbox: "Posteingang",
    navCalendar: "Kalender",
    navContacts: "Kontakte",
    eyebrow: "Web-Client",
    title: "Moderne JMAP-orientierte Oberflache",
    cardTitle: "Willkommen bei LPE",
    cardBody: "Dieser Web-Client bildet die Grundlage fur E-Mail, Kalender und Kontakte uber eine moderne API.",
    languageLabel: "Sprache"
  },
  it: {
    navInbox: "Posta in arrivo",
    navCalendar: "Calendario",
    navContacts: "Contatti",
    eyebrow: "Client web",
    title: "Interfaccia moderna orientata a JMAP",
    cardTitle: "Benvenuto in LPE",
    cardBody: "Questo client web costituisce la base per posta, calendari e contatti tramite una API moderna.",
    languageLabel: "Lingua"
  },
  es: {
    navInbox: "Bandeja de entrada",
    navCalendar: "Calendario",
    navContacts: "Contactos",
    eyebrow: "Cliente web",
    title: "Interfaz moderna orientada a JMAP",
    cardTitle: "Bienvenido a LPE",
    cardBody: "Este cliente web sirve de base para correo, calendarios y contactos mediante una API moderna.",
    languageLabel: "Idioma"
  }
} satisfies Record<
  Locale,
  {
    navInbox: string;
    navCalendar: string;
    navContacts: string;
    eyebrow: string;
    title: string;
    cardTitle: string;
    cardBody: string;
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
