export type Locale = "en" | "fr" | "de" | "it" | "es";
export type AppSection = "mail" | "calendar" | "contacts";
export type FolderKey = "focused" | "inbox" | "drafts" | "sent" | "archive";
export type MessageCategory = "priority" | "customer" | "internal";

type Messages = {
  productLabel: string;
  productTitle: string;
  compose: string;
  sectionLabel: string;
  sections: Record<AppSection, string>;
  sectionIcons: Record<AppSection, string>;
  mailboxLabel: string;
  folders: Record<FolderKey, string>;
  workspaceSummary: string;
  summaryInbox: string;
  summaryUnread: string;
  summaryAgenda: string;
  summaryAgendaCount: string;
  summaryFlagged: string;
  summaryFlaggedCount: string;
  searchPlaceholder: string;
  topActions: { sync: string; rules: string };
  languageLabel: string;
  heroEyebrow: string;
  heroTitle: string;
  heroBody: string;
  metrics: { reliability: string; search: string; searchValue: string; attachments: string };
  altViews: Record<Exclude<AppSection, "mail">, string>;
  messageCount: string;
  calendarCount: string;
  contactCount: string;
  noMessages: string;
  flaggedShort: string;
  categories: Record<MessageCategory, string>;
  attachmentCount: string;
  readingPane: string;
  messageActions: { reply: string; forward: string; archive: string };
  attachmentsTitle: string;
  attachmentsSubtitle: string;
  noAttachments: string;
  altDetailLabels: { calendar: string; contacts: string };
  calendarHeadline: string;
  calendarBody: string;
  calendarPoints: string[];
  contactsHeadline: string;
  contactsBody: string;
};

export const supportedLocales: Locale[] = ["en", "fr", "de", "it", "es"];

export const localeLabels: Record<Locale, string> = {
  en: "English",
  fr: "Francais",
  de: "Deutsch",
  it: "Italiano",
  es: "Espanol"
};

const en: Messages = {
  productLabel: "Outlook-style webmail",
  productTitle: "La Poste ELectronique",
  compose: "New message",
  sectionLabel: "Primary navigation",
  sections: { mail: "Mail", calendar: "Calendar", contacts: "People" },
  sectionIcons: { mail: "M", calendar: "C", contacts: "P" },
  mailboxLabel: "Folders",
  folders: { focused: "Focused", inbox: "Inbox", drafts: "Drafts", sent: "Sent", archive: "Archive" },
  workspaceSummary: "Workspace summary",
  summaryInbox: "Inbox health",
  summaryUnread: "{count} unread conversations",
  summaryAgenda: "Today's agenda",
  summaryAgendaCount: "{count} scheduled events",
  summaryFlagged: "Follow-up",
  summaryFlaggedCount: "{count} flagged threads",
  searchPlaceholder: "Search mail, files, contacts, or subjects",
  topActions: { sync: "Sync now", rules: "Mailbox rules" },
  languageLabel: "Language",
  heroEyebrow: "JMAP-first client",
  heroTitle: "Functional webmail for LPE users",
  heroBody: "This client gives users a practical mail workspace with focused triage, reading pane, agenda context, and attachment visibility aligned with the current LPE scope.",
  metrics: { reliability: "Mail delivery", search: "Search scope", searchValue: "Messages + attachments", attachments: "Indexed formats" },
  altViews: { calendar: "Today's calendar", contacts: "Directory" },
  messageCount: "{count} messages",
  calendarCount: "{count} events",
  contactCount: "{count} contacts",
  noMessages: "No messages match this folder and search.",
  flaggedShort: "Flagged",
  categories: { priority: "Priority", customer: "Customer", internal: "Internal" },
  attachmentCount: "{count} attachment(s)",
  readingPane: "Reading pane",
  messageActions: { reply: "Reply", forward: "Forward", archive: "Archive" },
  attachmentsTitle: "Attachments",
  attachmentsSubtitle: "Files visible from the selected conversation",
  noAttachments: "No attachments in this message.",
  altDetailLabels: { calendar: "Calendar view", contacts: "People view" },
  calendarHeadline: "A compact daily agenda beside mail",
  calendarBody: "The calendar panel keeps the current day visible without breaking the mail flow, which matches the productivity pattern expected from an Outlook-like workspace.",
  calendarPoints: ["Immediate view of the next meetings", "Room and attendee visibility", "Layout designed to coexist with the reading pane"],
  contactsHeadline: "Shared people directory",
  contactsBody: "Contacts stay close to the mail workflow so users can jump from a conversation to directory context without changing tools."
};

const fr: Messages = {
  productLabel: "Webmail inspire d'Outlook",
  productTitle: "La Poste ELectronique",
  compose: "Nouveau message",
  sectionLabel: "Navigation principale",
  sections: { mail: "Courrier", calendar: "Calendrier", contacts: "Contacts" },
  sectionIcons: { mail: "C", calendar: "A", contacts: "P" },
  mailboxLabel: "Dossiers",
  folders: { focused: "Prioritaire", inbox: "Boite de reception", drafts: "Brouillons", sent: "Envoyes", archive: "Archive" },
  workspaceSummary: "Resume de l'espace",
  summaryInbox: "Etat de la boite",
  summaryUnread: "{count} conversations non lues",
  summaryAgenda: "Agenda du jour",
  summaryAgendaCount: "{count} rendez-vous planifies",
  summaryFlagged: "Suivi",
  summaryFlaggedCount: "{count} fils signales",
  searchPlaceholder: "Rechercher des mails, fichiers, contacts ou sujets",
  topActions: { sync: "Synchroniser", rules: "Regles de boite" },
  languageLabel: "Langue",
  heroEyebrow: "Client oriente JMAP",
  heroTitle: "Un webmail fonctionnel pour les utilisateurs LPE",
  heroBody: "Cette interface fournit un espace mail exploitable avec tri prioritaire, panneau de lecture, contexte agenda et visibilite des pieces jointes dans le perimetre actuel de LPE.",
  metrics: { reliability: "Distribution mail", search: "Portee de recherche", searchValue: "Messages + pieces jointes", attachments: "Formats indexes" },
  altViews: { calendar: "Calendrier du jour", contacts: "Annuaire" },
  messageCount: "{count} messages",
  calendarCount: "{count} evenements",
  contactCount: "{count} contacts",
  noMessages: "Aucun message ne correspond a ce dossier et a cette recherche.",
  flaggedShort: "Suivi",
  categories: { priority: "Priorite", customer: "Client", internal: "Interne" },
  attachmentCount: "{count} piece(s) jointe(s)",
  readingPane: "Panneau de lecture",
  messageActions: { reply: "Repondre", forward: "Transferer", archive: "Archiver" },
  attachmentsTitle: "Pieces jointes",
  attachmentsSubtitle: "Fichiers visibles depuis la conversation selectionnee",
  noAttachments: "Aucune piece jointe dans ce message.",
  altDetailLabels: { calendar: "Vue calendrier", contacts: "Vue contacts" },
  calendarHeadline: "Un agenda quotidien compact a cote du mail",
  calendarBody: "Le panneau calendrier garde la journee visible sans casser le flux mail, ce qui correspond au modele de productivite attendu pour une interface proche d'Outlook.",
  calendarPoints: ["Vision immediate des prochains rendez-vous", "Visibilite des salles et participants", "Mise en page pensee pour cohabiter avec le panneau de lecture"],
  contactsHeadline: "Annuaire partage des personnes",
  contactsBody: "Les contacts restent proches du flux mail pour passer d'une conversation au contexte annuaire sans changer d'outil."
};

export const messages: Record<Locale, Messages> = {
  en,
  fr,
  de: { ...en, productLabel: "Outlook-inspiriertes Webmail", compose: "Neue Nachricht", sections: { mail: "E-Mail", calendar: "Kalender", contacts: "Kontakte" }, mailboxLabel: "Ordner", folders: { focused: "Relevant", inbox: "Posteingang", drafts: "Entwurfe", sent: "Gesendet", archive: "Archiv" }, workspaceSummary: "Arbeitsbereich", summaryInbox: "Posteingang", summaryUnread: "{count} ungelesene Gesprache", summaryAgenda: "Tagesplan", summaryAgendaCount: "{count} Termine", summaryFlagged: "Nachverfolgung", summaryFlaggedCount: "{count} markierte Threads", searchPlaceholder: "E-Mails, Dateien, Kontakte oder Betreff suchen", topActions: { sync: "Synchronisieren", rules: "Postfachregeln" }, languageLabel: "Sprache", heroEyebrow: "JMAP-orientierter Client", heroTitle: "Funktionales Webmail fur LPE-Benutzer", heroBody: "Diese Oberflache bietet einen nutzbaren Mail-Arbeitsplatz mit priorisierter Sicht, Lesebereich, Tagesagenda und sichtbaren Anhangen im aktuellen LPE-Umfang.", altViews: { calendar: "Kalender heute", contacts: "Verzeichnis" }, noMessages: "Keine Nachrichten passen zu diesem Ordner und dieser Suche.", flaggedShort: "Markiert", categories: { priority: "Prioritat", customer: "Kunde", internal: "Intern" }, readingPane: "Lesebereich", messageActions: { reply: "Antworten", forward: "Weiterleiten", archive: "Archivieren" }, attachmentsTitle: "Anhange", attachmentsSubtitle: "Dateien der ausgewahlten Konversation", noAttachments: "Keine Anhange in dieser Nachricht.", altDetailLabels: { calendar: "Kalenderansicht", contacts: "Kontaktansicht" }, calendarHeadline: "Kompakte Tagesagenda neben dem Mailbereich", calendarBody: "Der Kalender bleibt sichtbar, ohne den Mailfluss zu unterbrechen. Das passt zum erwarteten Produktivitatsmuster einer Outlook-ahnlichen Arbeitsflache.", calendarPoints: ["Sofortiger Blick auf die nachsten Termine", "Raume und Teilnehmer direkt sichtbar", "Layout fur parallele Nutzung mit dem Lesebereich"], contactsHeadline: "Gemeinsames Personenverzeichnis", contactsBody: "Kontakte bleiben nah am Mailfluss, damit Benutzer schnell vom Gesprach in den Verzeichnis-Kontext wechseln konnen." },
  it: { ...en, productLabel: "Webmail ispirato a Outlook", compose: "Nuovo messaggio", sections: { mail: "Posta", calendar: "Calendario", contacts: "Persone" }, mailboxLabel: "Cartelle", folders: { focused: "Prioritaria", inbox: "Posta in arrivo", drafts: "Bozze", sent: "Posta inviata", archive: "Archivio" }, workspaceSummary: "Riepilogo area", summaryInbox: "Stato inbox", summaryUnread: "{count} conversazioni non lette", summaryAgenda: "Agenda di oggi", summaryAgendaCount: "{count} eventi pianificati", summaryFlagged: "Da seguire", summaryFlaggedCount: "{count} thread contrassegnati", searchPlaceholder: "Cerca mail, file, contatti o oggetti", topActions: { sync: "Sincronizza", rules: "Regole casella" }, languageLabel: "Lingua", heroEyebrow: "Client orientato JMAP", heroTitle: "Webmail funzionale per gli utenti LPE", heroBody: "Questa interfaccia offre uno spazio mail concreto con triage prioritario, pannello di lettura, contesto agenda e visibilita degli allegati nel perimetro attuale di LPE.", altViews: { calendar: "Calendario di oggi", contacts: "Rubrica" }, noMessages: "Nessun messaggio corrisponde a questa cartella e a questa ricerca.", flaggedShort: "Seguito", categories: { priority: "Priorita", customer: "Cliente", internal: "Interno" }, readingPane: "Pannello di lettura", messageActions: { reply: "Rispondi", forward: "Inoltra", archive: "Archivia" }, attachmentsTitle: "Allegati", attachmentsSubtitle: "File visibili dalla conversazione selezionata", noAttachments: "Nessun allegato in questo messaggio.", altDetailLabels: { calendar: "Vista calendario", contacts: "Vista contatti" }, calendarHeadline: "Agenda giornaliera compatta accanto alla posta", calendarBody: "Il pannello calendario mantiene visibile la giornata senza interrompere il flusso mail, in linea con l'esperienza attesa da uno spazio simile a Outlook.", calendarPoints: ["Vista immediata dei prossimi incontri", "Sale e partecipanti visibili", "Layout pensato per convivere con il pannello di lettura"], contactsHeadline: "Rubrica condivisa", contactsBody: "I contatti restano vicini al flusso mail cosi l'utente passa dalla conversazione al contesto rubrica senza cambiare strumento." },
  es: { ...en, productLabel: "Webmail inspirado en Outlook", compose: "Nuevo mensaje", sections: { mail: "Correo", calendar: "Calendario", contacts: "Personas" }, mailboxLabel: "Carpetas", folders: { focused: "Prioritarios", inbox: "Bandeja de entrada", drafts: "Borradores", sent: "Enviados", archive: "Archivo" }, workspaceSummary: "Resumen del espacio", summaryInbox: "Estado de entrada", summaryUnread: "{count} conversaciones sin leer", summaryAgenda: "Agenda de hoy", summaryAgendaCount: "{count} eventos programados", summaryFlagged: "Seguimiento", summaryFlaggedCount: "{count} hilos marcados", searchPlaceholder: "Buscar correos, archivos, contactos o asuntos", topActions: { sync: "Sincronizar", rules: "Reglas del buzon" }, languageLabel: "Idioma", heroEyebrow: "Cliente orientado a JMAP", heroTitle: "Webmail funcional para usuarios de LPE", heroBody: "Esta interfaz ofrece un espacio de correo util con triage prioritario, panel de lectura, contexto de agenda y visibilidad de adjuntos dentro del alcance actual de LPE.", altViews: { calendar: "Calendario de hoy", contacts: "Directorio" }, noMessages: "Ningun mensaje coincide con esta carpeta y busqueda.", flaggedShort: "Marcado", categories: { priority: "Prioridad", customer: "Cliente", internal: "Interno" }, readingPane: "Panel de lectura", messageActions: { reply: "Responder", forward: "Reenviar", archive: "Archivar" }, attachmentsTitle: "Adjuntos", attachmentsSubtitle: "Archivos visibles desde la conversacion seleccionada", noAttachments: "No hay adjuntos en este mensaje.", altDetailLabels: { calendar: "Vista calendario", contacts: "Vista personas" }, calendarHeadline: "Agenda diaria compacta junto al correo", calendarBody: "El panel de calendario mantiene el dia visible sin romper el flujo del correo, como se espera de un espacio de trabajo inspirado en Outlook.", calendarPoints: ["Vista inmediata de las proximas reuniones", "Salas y asistentes visibles", "Diseno pensado para convivir con el panel de lectura"], contactsHeadline: "Directorio compartido de personas", contactsBody: "Los contactos permanecen cerca del flujo del correo para pasar de una conversacion al contexto del directorio sin cambiar de herramienta." }
};

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
