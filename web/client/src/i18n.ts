export type Locale = "en" | "fr" | "de" | "it" | "es";
export type AppSection = "mail" | "calendar" | "contacts";
export type FolderKey = "focused" | "inbox" | "drafts" | "sent" | "archive";
export type MessageCategory = "priority" | "customer" | "internal";

type Copy = {
  productTitle: string; productSubtitle: string; compose: string; sectionLabel: string; sections: Record<AppSection, string>; sectionIcons: Record<AppSection, string>;
  mailboxLabel: string; folders: Record<FolderKey, string>; workspaceSummary: string; summaryInbox: string; summaryUnread: string; summaryAgenda: string; summaryAgendaCount: string; summaryDrafts: string; summaryDraftsCount: string;
  searchPlaceholder: string; topActions: { sync: string; rules: string; schedule: string }; languageLabel: string; heroEyebrow: string; heroTitle: string; heroBody: string;
  metrics: { reliability: string; search: string; searchValue: string; attachments: string; workflow: string; workflowValue: string };
  altViews: Record<AppSection, string>; messageCount: string; calendarCount: string; contactCount: string; noMessages: string; flaggedShort: string; categories: Record<MessageCategory, string>; attachmentCount: string;
  readingPane: string; messageActions: { reply: string; forward: string; archive: string }; attachmentsTitle: string; attachmentsSubtitle: string; noAttachments: string;
  altDetailLabels: { calendar: string; contacts: string }; editorLabel: string; editorTitles: Record<"new" | "reply" | "forward", string>; editorActions: { saveDraft: string; cancel: string; send: string };
  fields: { to: string; cc: string; subject: string; body: string }; nowLabel: string; untitledDraft: string; emptyDraftPreview: string; tags: { draft: string; reply: string; forward: string; outgoing: string };
  noticeSent: string; noticeDraft: string; validationMessage: string; calendarEditTitle: string; calendarCreateTitle: string; calendarActions: { new: string; save: string; create: string }; calendarFields: { date: string; time: string; title: string; location: string; attendees: string; notes: string }; noticeCalendarUpdated: string; noticeCalendarCreated: string; validationCalendar: string;
  contactsEditTitle: string; contactsCreateTitle: string; contactActions: { new: string; save: string; create: string }; contactFields: { name: string; role: string; email: string; phone: string; team: string; notes: string }; noticeContactUpdated: string; noticeContactCreated: string; validationContact: string;
};

export type ClientCopy = Copy;

export const supportedLocales: Locale[] = ["en", "fr", "de", "it", "es"];
export const localeLabels: Record<Locale, string> = { en: "English", fr: "Francais", de: "Deutsch", it: "Italiano", es: "Espanol" };

const en: Copy = {
  productTitle: "La Poste ELectronique", productSubtitle: "Mail, calendar, and people workspace", compose: "New message", sectionLabel: "Primary navigation", sections: { mail: "Mail", calendar: "Calendar", contacts: "People" }, sectionIcons: { mail: "M", calendar: "C", contacts: "P" },
  mailboxLabel: "Folders", folders: { focused: "Focused", inbox: "Inbox", drafts: "Drafts", sent: "Sent", archive: "Archive" }, workspaceSummary: "Workspace summary", summaryInbox: "Inbox health", summaryUnread: "{count} unread conversations", summaryAgenda: "Today's agenda", summaryAgendaCount: "{count} scheduled events", summaryDrafts: "Drafts", summaryDraftsCount: "{count} saved drafts",
  searchPlaceholder: "Search mail, files, contacts, or subjects", topActions: { sync: "Sync now", rules: "Mailbox rules", schedule: "Plan meeting" }, languageLabel: "Language", heroEyebrow: "JMAP-first workspace", heroTitle: "A fuller professional client for daily communication", heroBody: "This version adds message composition, reply and forward flows, editable contacts, and editable calendar entries in a denser three-pane workspace.",
  metrics: { reliability: "Mail delivery", search: "Search scope", searchValue: "Messages + attachments", attachments: "Indexed formats", workflow: "Core actions", workflowValue: "Compose, edit, plan" },
  altViews: { mail: "Mail", calendar: "Agenda", contacts: "Directory" }, messageCount: "{count} messages", calendarCount: "{count} events", contactCount: "{count} contacts", noMessages: "No messages match this folder and search.", flaggedShort: "Flagged", categories: { priority: "Priority", customer: "Customer", internal: "Internal" }, attachmentCount: "{count} attachment(s)",
  readingPane: "Reading pane", messageActions: { reply: "Reply", forward: "Forward", archive: "Archive" }, attachmentsTitle: "Attachments", attachmentsSubtitle: "Files visible from the selected conversation", noAttachments: "No attachments in this message.",
  altDetailLabels: { calendar: "Calendar editor", contacts: "People editor" }, editorLabel: "Message editor", editorTitles: { new: "Compose message", reply: "Reply to message", forward: "Forward message" }, editorActions: { saveDraft: "Save draft", cancel: "Cancel", send: "Send" },
  fields: { to: "To", cc: "Cc", subject: "Subject", body: "Body" }, nowLabel: "Now", untitledDraft: "Untitled draft", emptyDraftPreview: "Draft message", tags: { draft: "Draft", reply: "Reply", forward: "Forward", outgoing: "Outgoing" },
  noticeSent: "Message sent.", noticeDraft: "Draft saved.", validationMessage: "Fill at least the recipient for send, and some subject or body content.",
  calendarEditTitle: "Edit event", calendarCreateTitle: "Create event", calendarActions: { new: "New event", save: "Save changes", create: "Create event" }, calendarFields: { date: "Date", time: "Time", title: "Title", location: "Location", attendees: "Attendees", notes: "Notes" }, noticeCalendarUpdated: "Event updated.", noticeCalendarCreated: "Event created.", validationCalendar: "Fill date, time, and title.",
  contactsEditTitle: "Edit contact", contactsCreateTitle: "Create contact", contactActions: { new: "New contact", save: "Save changes", create: "Create contact" }, contactFields: { name: "Name", role: "Role", email: "Email", phone: "Phone", team: "Team", notes: "Notes" }, noticeContactUpdated: "Contact updated.", noticeContactCreated: "Contact created.", validationContact: "Fill at least name and email."
};

const fr: Copy = {
  productTitle: "La Poste ELectronique", productSubtitle: "Espace courrier, calendrier et contacts", compose: "Nouveau message", sectionLabel: "Navigation principale", sections: { mail: "Courrier", calendar: "Calendrier", contacts: "Contacts" }, sectionIcons: { mail: "C", calendar: "A", contacts: "P" },
  mailboxLabel: "Dossiers", folders: { focused: "Prioritaire", inbox: "Boite de reception", drafts: "Brouillons", sent: "Envoyes", archive: "Archive" }, workspaceSummary: "Resume de l'espace", summaryInbox: "Etat de la boite", summaryUnread: "{count} conversations non lues", summaryAgenda: "Agenda du jour", summaryAgendaCount: "{count} rendez-vous planifies", summaryDrafts: "Brouillons", summaryDraftsCount: "{count} brouillons enregistres",
  searchPlaceholder: "Rechercher des mails, fichiers, contacts ou sujets", topActions: { sync: "Synchroniser", rules: "Regles de boite", schedule: "Planifier" }, languageLabel: "Langue", heroEyebrow: "Espace oriente JMAP", heroTitle: "Une interface plus complete et plus professionnelle", heroBody: "Cette version ajoute la composition d'email, les flux reply et forward, l'edition des contacts et l'edition des entrees calendrier dans une vraie disposition de travail.",
  metrics: { reliability: "Distribution mail", search: "Portee de recherche", searchValue: "Messages + pieces jointes", attachments: "Formats indexes", workflow: "Actions clefs", workflowValue: "Rediger, modifier, planifier" },
  altViews: { mail: "Courrier", calendar: "Agenda", contacts: "Annuaire" }, messageCount: "{count} messages", calendarCount: "{count} evenements", contactCount: "{count} contacts", noMessages: "Aucun message ne correspond a ce dossier et a cette recherche.", flaggedShort: "Suivi", categories: { priority: "Priorite", customer: "Client", internal: "Interne" }, attachmentCount: "{count} piece(s) jointe(s)",
  readingPane: "Panneau de lecture", messageActions: { reply: "Repondre", forward: "Transferer", archive: "Archiver" }, attachmentsTitle: "Pieces jointes", attachmentsSubtitle: "Fichiers visibles depuis la conversation selectionnee", noAttachments: "Aucune piece jointe dans ce message.",
  altDetailLabels: { calendar: "Edition calendrier", contacts: "Edition contacts" }, editorLabel: "Edition du message", editorTitles: { new: "Rediger un message", reply: "Repondre au message", forward: "Transferer le message" }, editorActions: { saveDraft: "Enregistrer", cancel: "Annuler", send: "Envoyer" },
  fields: { to: "A", cc: "Cc", subject: "Sujet", body: "Contenu" }, nowLabel: "Maintenant", untitledDraft: "Brouillon sans titre", emptyDraftPreview: "Message en brouillon", tags: { draft: "Brouillon", reply: "Reponse", forward: "Transfert", outgoing: "Sortant" },
  noticeSent: "Message envoye.", noticeDraft: "Brouillon enregistre.", validationMessage: "Renseigne au moins le destinataire pour l'envoi, et un sujet ou du contenu.",
  calendarEditTitle: "Modifier l'evenement", calendarCreateTitle: "Creer un evenement", calendarActions: { new: "Nouvel evenement", save: "Enregistrer", create: "Creer" }, calendarFields: { date: "Date", time: "Heure", title: "Titre", location: "Lieu", attendees: "Participants", notes: "Notes" }, noticeCalendarUpdated: "Evenement mis a jour.", noticeCalendarCreated: "Evenement cree.", validationCalendar: "Renseigne date, heure et titre.",
  contactsEditTitle: "Modifier le contact", contactsCreateTitle: "Creer un contact", contactActions: { new: "Nouveau contact", save: "Enregistrer", create: "Creer" }, contactFields: { name: "Nom", role: "Role", email: "Email", phone: "Telephone", team: "Equipe", notes: "Notes" }, noticeContactUpdated: "Contact mis a jour.", noticeContactCreated: "Contact cree.", validationContact: "Renseigne au moins le nom et l'email."
};

export const messages: Record<Locale, Copy> = { en, fr, de: en, it: en, es: en };
export function getInitialLocale(): Locale { if (typeof window === "undefined") return "en"; const stored = window.localStorage.getItem("lpe.locale"); return stored && supportedLocales.includes(stored as Locale) ? stored as Locale : "en"; }
