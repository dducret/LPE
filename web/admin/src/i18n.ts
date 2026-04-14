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
    languageLabel: "Language",
    apiStatusTitle: "API status",
    apiStatusBody: "The administration console is served by nginx and consumes the local Rust API through /api/.",
    serviceHealthy: "Service healthy",
    serviceUnhealthy: "Service degraded",
    localAiTitle: "Local AI readiness",
    localAiBody: "The backend already exposes a local-only AI health probe and a bootstrap summary payload.",
    adminAccountTitle: "Bootstrap administrator",
    adminAccountBody: "Default bootstrap identity returned by the current API surface.",
    attachmentTitle: "Indexed attachment formats",
    attachmentBody: "Formats currently exposed by the attachment capability endpoint.",
    loading: "Loading live status...",
    failed: "Unable to reach the administration API through nginx.",
    providerLabel: "Provider",
    modelsLabel: "Models",
    endpointLabel: "Endpoint",
    openApiLabel: "Open API health endpoint"
  },
  fr: {
    eyebrow: "La Poste ELectronique",
    title: "Console d'administration",
    body: "Point d'entree initial du back office pour piloter comptes, domaines, quotas et politiques.",
    languageLabel: "Langue",
    apiStatusTitle: "Etat de l'API",
    apiStatusBody: "La console d'administration est servie par nginx et consomme l'API Rust locale via /api/.",
    serviceHealthy: "Service operationnel",
    serviceUnhealthy: "Service degrade",
    localAiTitle: "Preparation IA locale",
    localAiBody: "Le backend expose deja une sonde d'etat IA locale et une charge utile de resume d'amorcage.",
    adminAccountTitle: "Administrateur d'amorcage",
    adminAccountBody: "Identite d'amorcage par defaut renvoyee par la surface API actuelle.",
    attachmentTitle: "Formats de pieces jointes indexes",
    attachmentBody: "Formats actuellement exposes par le endpoint de capacites des pieces jointes.",
    loading: "Chargement de l'etat en direct...",
    failed: "Impossible de joindre l'API d'administration via nginx.",
    providerLabel: "Fournisseur",
    modelsLabel: "Modeles",
    endpointLabel: "Endpoint",
    openApiLabel: "Ouvrir le endpoint de sante API"
  },
  de: {
    eyebrow: "La Poste ELectronique",
    title: "Administrationskonsole",
    body: "Erster Backoffice-Einstiegspunkt zur Verwaltung von Konten, Domains, Quoten und Richtlinien.",
    languageLabel: "Sprache",
    apiStatusTitle: "API-Status",
    apiStatusBody: "Die Administrationskonsole wird von nginx ausgeliefert und nutzt die lokale Rust-API ueber /api/.",
    serviceHealthy: "Dienst verfuegbar",
    serviceUnhealthy: "Dienst beeintraechtigt",
    localAiTitle: "Lokale KI-Bereitschaft",
    localAiBody: "Das Backend bietet bereits einen lokalen KI-Health-Check und eine Bootstrap-Zusammenfassung.",
    adminAccountTitle: "Bootstrap-Administrator",
    adminAccountBody: "Standard-Bootstrap-Identitaet der aktuellen API-Oberflaeche.",
    attachmentTitle: "Indizierte Anhangsformate",
    attachmentBody: "Formate, die aktuell ueber den Attachment-Capability-Endpunkt gemeldet werden.",
    loading: "Live-Status wird geladen...",
    failed: "Die Administrations-API ist ueber nginx nicht erreichbar.",
    providerLabel: "Provider",
    modelsLabel: "Modelle",
    endpointLabel: "Endpunkt",
    openApiLabel: "API-Health-Endpunkt oeffnen"
  },
  it: {
    eyebrow: "La Poste ELectronique",
    title: "Console di amministrazione",
    body: "Punto di accesso iniziale del back office per gestire account, domini, quote e criteri.",
    languageLabel: "Lingua",
    apiStatusTitle: "Stato API",
    apiStatusBody: "La console di amministrazione e servita da nginx e usa l'API Rust locale tramite /api/.",
    serviceHealthy: "Servizio operativo",
    serviceUnhealthy: "Servizio degradato",
    localAiTitle: "Prontezza IA locale",
    localAiBody: "Il backend espone gia un controllo di salute IA locale e un payload di riepilogo bootstrap.",
    adminAccountTitle: "Amministratore bootstrap",
    adminAccountBody: "Identita bootstrap predefinita restituita dall'attuale superficie API.",
    attachmentTitle: "Formati allegati indicizzati",
    attachmentBody: "Formati attualmente esposti dall'endpoint delle capacita allegati.",
    loading: "Caricamento dello stato in tempo reale...",
    failed: "Impossibile raggiungere l'API di amministrazione tramite nginx.",
    providerLabel: "Provider",
    modelsLabel: "Modelli",
    endpointLabel: "Endpoint",
    openApiLabel: "Apri endpoint salute API"
  },
  es: {
    eyebrow: "La Poste ELectronique",
    title: "Consola de administracion",
    body: "Punto de entrada inicial del back office para gestionar cuentas, dominios, cuotas y politicas.",
    languageLabel: "Idioma",
    apiStatusTitle: "Estado de la API",
    apiStatusBody: "La consola de administracion se sirve con nginx y consume la API Rust local a traves de /api/.",
    serviceHealthy: "Servicio operativo",
    serviceUnhealthy: "Servicio degradado",
    localAiTitle: "Preparacion IA local",
    localAiBody: "El backend ya expone una sonda de salud de IA local y una carga util de resumen de bootstrap.",
    adminAccountTitle: "Administrador bootstrap",
    adminAccountBody: "Identidad bootstrap predeterminada devuelta por la superficie actual de la API.",
    attachmentTitle: "Formatos de adjuntos indexados",
    attachmentBody: "Formatos expuestos actualmente por el endpoint de capacidades de adjuntos.",
    loading: "Cargando estado en vivo...",
    failed: "No se puede acceder a la API de administracion a traves de nginx.",
    providerLabel: "Proveedor",
    modelsLabel: "Modelos",
    endpointLabel: "Endpoint",
    openApiLabel: "Abrir endpoint de salud API"
  }
} satisfies Record<
  Locale,
  {
    eyebrow: string;
    title: string;
    body: string;
    languageLabel: string;
    apiStatusTitle: string;
    apiStatusBody: string;
    serviceHealthy: string;
    serviceUnhealthy: string;
    localAiTitle: string;
    localAiBody: string;
    adminAccountTitle: string;
    adminAccountBody: string;
    attachmentTitle: string;
    attachmentBody: string;
    loading: string;
    failed: string;
    providerLabel: string;
    modelsLabel: string;
    endpointLabel: string;
    openApiLabel: string;
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
