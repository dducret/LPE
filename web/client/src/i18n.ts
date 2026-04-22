import {
  defineLocaleCatalog,
  getInitialLocale,
  localeLabels,
  setStoredLocale,
  supportedLocales,
  type Locale,
} from "../../shared/src/i18n";

export { getInitialLocale, localeLabels, setStoredLocale, supportedLocales, type Locale };
export type AppSection = "mail" | "calendar" | "contacts" | "settings";
export type FolderKey = "focused" | "inbox" | "drafts" | "sent" | "archive";

type Copy = {
  productTitle: string; productSubtitle: string; compose: string; sectionLabel: string; sections: Record<AppSection, string>; sectionIcons: Record<AppSection, string>;
  loginTitle: string; loginHelp: string; loginEmail: string; loginPassword: string; loginTotp: string; loginSubmit: string; loginOidc: string; loginOrDivider: string; loginError: string; logout: string; signedInAs: string;
  accountMenuLabel: string; accountMenuTitle: string;
  shellTabs: string[]; ribbonActions: string[]; ribbonSecondary: string[]; toolbarChips: string[];
  favoritesLabel: string; rightPaneTitle: string;
  mailboxLabel: string; folders: Record<FolderKey, string>; workspaceSummary: string; summaryInbox: string; summaryUnread: string; summaryAgenda: string; summaryAgendaCount: string; summaryDrafts: string; summaryDraftsCount: string;
  searchPlaceholder: string; topActions: { sync: string }; languageLabel: string; heroEyebrow: string; heroTitle: string; heroBody: string; calendarBody: string; contactsBody: string;
  metrics: { reliability: string; search: string; searchValue: string; attachments: string; workflow: string; workflowValue: string };
  listColumns: { from: string; subject: string; received: string };
  altViews: Record<AppSection, string>; messageCount: string; calendarCount: string; contactCount: string; noMessages: string; noCalendarEvents: string; noContacts: string; loadingWorkspace: string; loadError: string; saveError: string; flaggedShort: string; attachmentCount: string;
  readingPane: string; drawerTitle: string; messageActions: { reply: string; forward: string; archive: string }; attachmentsTitle: string; attachmentsSubtitle: string; noAttachments: string;
  altDetailLabels: { calendar: string; contacts: string }; editorLabel: string; editorTitles: Record<"new" | "draft" | "reply" | "forward", string>; editorActions: { saveDraft: string; deleteDraft: string; cancel: string; send: string };
  fields: { from: string; senderMode: string; to: string; cc: string; subject: string; body: string }; senderModes: { sendAs: string; sendOnBehalf: string }; nowLabel: string; untitledDraft: string; emptyDraftPreview: string; tags: { draft: string; reply: string; forward: string; outgoing: string };
  noticeSent: string; noticeDraft: string; noticeDraftDeleted: string; noticeSyncDone: string; validationMessage: string; calendarEditTitle: string; calendarCreateTitle: string; calendarActions: { new: string; save: string; create: string }; calendarFields: { date: string; time: string; title: string; location: string; attendees: string; notes: string }; noticeCalendarUpdated: string; noticeCalendarCreated: string; validationCalendar: string;
  contactsEditTitle: string; contactsCreateTitle: string; contactActions: { new: string; save: string; create: string }; contactFields: { name: string; role: string; email: string; phone: string; team: string; notes: string }; noticeContactUpdated: string; noticeContactCreated: string; validationContact: string;
  settings: {
    delegationLabel: string; delegationTitle: string; outgoingSharesTitle: string; incomingSharesTitle: string; mailboxDelegationTitle: string;
    collectionField: string; taskListField: string; granteeEmailField: string; mailboxGranteeField: string; senderRightField: string;
    collectionKinds: { contacts: string; calendar: string; tasks: string };
    rights: { read: string; write: string; delete: string; share: string };
    actions: { saveShare: string; shareInbox: string; grantSenderRight: string };
    senderRights: { sendAs: string; sendOnBehalf: string };
    inboxAccess: string; remove: string; taskListPrefix: string;
    sieveLabel: string; sieveTitle: string; scriptNameField: string; activateAfterSaveField: string; sieveScriptField: string; storedScriptsTitle: string; bytesLabel: string;
    scriptState: { active: string; inactive: string };
    sieveActions: { saveScript: string; disableActive: string; load: string; activate: string; delete: string };
    shareUpdated: string; shareRemoved: string; validationTaskList: string;
  };
};

export type ClientCopy = Copy;

const en: Copy = {
  productTitle: "La Poste Electronique", productSubtitle: "Mail, calendar, and people workspace", compose: "New message", sectionLabel: "Primary navigation", sections: { mail: "Mail", calendar: "Calendar", contacts: "People", settings: "Settings" }, sectionIcons: { mail: "M", calendar: "C", contacts: "P", settings: "S" },
  loginTitle: "Sign in to mail", loginHelp: "Use the mailbox email address and password configured by the administrator. Add the TOTP code when mailbox MFA is enabled.", loginEmail: "Email address", loginPassword: "Password", loginTotp: "TOTP code", loginSubmit: "Sign in", loginOidc: "Continue with SSO", loginOrDivider: "or use mailbox SSO", loginError: "Invalid mailbox credentials.", logout: "Sign out", signedInAs: "Signed in as {email}", accountMenuLabel: "Account", accountMenuTitle: "Account menu",
  shellTabs: ["File", "Home", "View", "Help"], ribbonActions: ["Delete", "Archive", "Report", "Sweep", "Move to"], ribbonSecondary: ["Read / Unread", "Flag", "Pin", "Print"], toolbarChips: ["Select", "Filter", "By date"], favoritesLabel: "Favorites", rightPaneTitle: "Focused message",
  mailboxLabel: "Folders", folders: { focused: "Focused", inbox: "Inbox", drafts: "Drafts", sent: "Sent", archive: "Archive" }, workspaceSummary: "Workspace summary", summaryInbox: "Inbox health", summaryUnread: "{count} unread conversations", summaryAgenda: "Today's agenda", summaryAgendaCount: "{count} scheduled events", summaryDrafts: "Drafts", summaryDraftsCount: "{count} saved drafts",
  searchPlaceholder: "Search mail, files, contacts, or subjects", topActions: { sync: "Sync now" }, languageLabel: "Language", heroEyebrow: "Mailbox workspace", heroTitle: "Current account state", heroBody: "Mail, drafts, contacts, and calendar entries come directly from the canonical LPE account data.", calendarBody: "Calendar entries shown here are the persisted events stored for this account.", contactsBody: "Contacts shown here are the persisted people records stored for this account.",
  metrics: { reliability: "Mail delivery", search: "Search scope", searchValue: "Messages + attachments", attachments: "Indexed formats", workflow: "Core actions", workflowValue: "Compose, edit, plan" },
  listColumns: { from: "From", subject: "Subject", received: "Received" },
  altViews: { mail: "Mail", calendar: "Agenda", contacts: "Directory", settings: "Settings" }, messageCount: "{count} messages", calendarCount: "{count} events", contactCount: "{count} contacts", noMessages: "No messages match this folder and search.", noCalendarEvents: "No calendar events stored for this account.", noContacts: "No contacts stored for this account.", loadingWorkspace: "Loading account data.", loadError: "Unable to load account data.", saveError: "Unable to save this change.", flaggedShort: "Flagged", attachmentCount: "{count} attachment(s)",
  readingPane: "Reading pane", drawerTitle: "Details drawer", messageActions: { reply: "Reply", forward: "Forward", archive: "Archive" }, attachmentsTitle: "Attachments", attachmentsSubtitle: "Files visible from the selected conversation", noAttachments: "No attachments in this message.",
  altDetailLabels: { calendar: "Calendar editor", contacts: "People editor" }, editorLabel: "Message editor", editorTitles: { new: "Compose message", draft: "Edit draft", reply: "Reply to message", forward: "Forward message" }, editorActions: { saveDraft: "Save draft", deleteDraft: "Delete draft", cancel: "Cancel", send: "Send" },
  fields: { from: "From", senderMode: "Sender mode", to: "To", cc: "Cc", subject: "Subject", body: "Body" }, senderModes: { sendAs: "Send as mailbox", sendOnBehalf: "Send on behalf" }, nowLabel: "Now", untitledDraft: "Untitled draft", emptyDraftPreview: "Draft message", tags: { draft: "Draft", reply: "Reply", forward: "Forward", outgoing: "Outgoing" },
  noticeSent: "Message sent.", noticeDraft: "Draft saved.", noticeDraftDeleted: "Draft deleted.", noticeSyncDone: "Mailbox refreshed.", validationMessage: "Fill at least the recipient for send, and some subject or body content.",
  calendarEditTitle: "Edit event", calendarCreateTitle: "Create event", calendarActions: { new: "New event", save: "Save changes", create: "Create event" }, calendarFields: { date: "Date", time: "Time", title: "Title", location: "Location", attendees: "Attendees", notes: "Notes" }, noticeCalendarUpdated: "Event updated.", noticeCalendarCreated: "Event created.", validationCalendar: "Fill date, time, and title.",
  contactsEditTitle: "Edit contact", contactsCreateTitle: "Create contact", contactActions: { new: "New contact", save: "Save changes", create: "Create contact" }, contactFields: { name: "Name", role: "Role", email: "Email", phone: "Phone", team: "Team", notes: "Notes" }, noticeContactUpdated: "Contact updated.", noticeContactCreated: "Contact created.", validationContact: "Fill at least name and email.",
  settings: {
    delegationLabel: "Delegation", delegationTitle: "Mailbox and collection access", outgoingSharesTitle: "Outgoing shares", incomingSharesTitle: "Incoming shared collections", mailboxDelegationTitle: "Mailbox delegation",
    collectionField: "Collection", taskListField: "Task list", granteeEmailField: "Grantee email", mailboxGranteeField: "Mailbox grantee", senderRightField: "Sender right",
    collectionKinds: { contacts: "Contacts", calendar: "Calendar", tasks: "Task list" },
    rights: { read: "Read", write: "Write", delete: "Delete", share: "Share" },
    actions: { saveShare: "Save share", shareInbox: "Share inbox", grantSenderRight: "Grant sender right" },
    senderRights: { sendAs: "Send As", sendOnBehalf: "Send on Behalf" },
    inboxAccess: "Inbox access", remove: "Remove", taskListPrefix: "Task list {name}",
    sieveLabel: "Sieve", sieveTitle: "Vacation and filtering", scriptNameField: "Script name", activateAfterSaveField: "Activate after save", sieveScriptField: "Sieve script", storedScriptsTitle: "Stored scripts", bytesLabel: "bytes",
    scriptState: { active: "active", inactive: "inactive" },
    sieveActions: { saveScript: "Save script", disableActive: "Disable active", load: "Load", activate: "Activate", delete: "Delete" },
    shareUpdated: "Share updated.", shareRemoved: "Share removed.", validationTaskList: "Select a task list to share."
  }
};

const fr: Copy = {
  productTitle: "La Poste Electronique", productSubtitle: "Espace courrier, calendrier et contacts", compose: "Nouveau message", sectionLabel: "Navigation principale", sections: { mail: "Courrier", calendar: "Calendrier", contacts: "Contacts", settings: "Parametres" }, sectionIcons: { mail: "C", calendar: "A", contacts: "P", settings: "R" },
  loginTitle: "Connexion au courrier", loginHelp: "Utilise l'adresse email de la boite et le mot de passe configure par l'administrateur. Ajoute le code TOTP quand la MFA mailbox est active.", loginEmail: "Adresse email", loginPassword: "Mot de passe", loginTotp: "Code TOTP", loginSubmit: "Se connecter", loginOidc: "Continuer avec le SSO", loginOrDivider: "ou utiliser le SSO mailbox", loginError: "Identifiants mailbox invalides.", logout: "Se deconnecter", signedInAs: "Connecte en tant que {email}", accountMenuLabel: "Compte", accountMenuTitle: "Menu du compte",
  shellTabs: ["Fichier", "Accueil", "Affichage", "Aide"], ribbonActions: ["Supprimer", "Archiver", "Signaler", "Nettoyer", "Deplacer"], ribbonSecondary: ["Lu / Non lu", "Drapeau", "Epingler", "Imprimer"], toolbarChips: ["Selection", "Filtrer", "Par date"], favoritesLabel: "Favoris", rightPaneTitle: "Message prioritaire",
  mailboxLabel: "Dossiers", folders: { focused: "Prioritaire", inbox: "Boite de reception", drafts: "Brouillons", sent: "Envoyes", archive: "Archive" }, workspaceSummary: "Resume de l'espace", summaryInbox: "Etat de la boite", summaryUnread: "{count} conversations non lues", summaryAgenda: "Agenda du jour", summaryAgendaCount: "{count} rendez-vous planifies", summaryDrafts: "Brouillons", summaryDraftsCount: "{count} brouillons enregistres",
  searchPlaceholder: "Rechercher des mails, fichiers, contacts ou sujets", topActions: { sync: "Synchroniser" }, languageLabel: "Langue", heroEyebrow: "Espace mailbox", heroTitle: "Etat actuel du compte", heroBody: "Les emails, brouillons, contacts et evenements affiches ici proviennent directement des donnees canoniques du compte LPE.", calendarBody: "Les evenements affiches ici correspondent aux donnees calendrier persistantes de ce compte.", contactsBody: "Les contacts affiches ici correspondent aux fiches personnes persistantes de ce compte.",
  metrics: { reliability: "Distribution mail", search: "Portee de recherche", searchValue: "Messages + pieces jointes", attachments: "Formats indexes", workflow: "Actions clefs", workflowValue: "Rediger, modifier, planifier" },
  listColumns: { from: "Expediteur", subject: "Sujet", received: "Recu" },
  altViews: { mail: "Courrier", calendar: "Agenda", contacts: "Annuaire", settings: "Parametres" }, messageCount: "{count} messages", calendarCount: "{count} evenements", contactCount: "{count} contacts", noMessages: "Aucun message ne correspond a ce dossier et a cette recherche.", noCalendarEvents: "Aucun evenement calendrier enregistre pour ce compte.", noContacts: "Aucun contact enregistre pour ce compte.", loadingWorkspace: "Chargement des donnees du compte.", loadError: "Impossible de charger les donnees du compte.", saveError: "Impossible d'enregistrer cette modification.", flaggedShort: "Suivi", attachmentCount: "{count} piece(s) jointe(s)",
  readingPane: "Panneau de lecture", drawerTitle: "Drawer de details", messageActions: { reply: "Repondre", forward: "Transferer", archive: "Archiver" }, attachmentsTitle: "Pieces jointes", attachmentsSubtitle: "Fichiers visibles depuis la conversation selectionnee", noAttachments: "Aucune piece jointe dans ce message.",
  altDetailLabels: { calendar: "Edition calendrier", contacts: "Edition contacts" }, editorLabel: "Edition du message", editorTitles: { new: "Rediger un message", draft: "Modifier le brouillon", reply: "Repondre au message", forward: "Transferer le message" }, editorActions: { saveDraft: "Enregistrer", deleteDraft: "Supprimer le brouillon", cancel: "Annuler", send: "Envoyer" },
  fields: { from: "De", senderMode: "Mode d'envoi", to: "A", cc: "Cc", subject: "Sujet", body: "Contenu" }, senderModes: { sendAs: "Envoyer comme la boite", sendOnBehalf: "Envoyer au nom de" }, nowLabel: "Maintenant", untitledDraft: "Brouillon sans titre", emptyDraftPreview: "Message en brouillon", tags: { draft: "Brouillon", reply: "Reponse", forward: "Transfert", outgoing: "Sortant" },
  noticeSent: "Message envoye.", noticeDraft: "Brouillon enregistre.", noticeDraftDeleted: "Brouillon supprime.", noticeSyncDone: "Boite rechargée.", validationMessage: "Renseigne au moins le destinataire pour l'envoi, et un sujet ou du contenu.",
  calendarEditTitle: "Modifier l'evenement", calendarCreateTitle: "Creer un evenement", calendarActions: { new: "Nouvel evenement", save: "Enregistrer", create: "Creer" }, calendarFields: { date: "Date", time: "Heure", title: "Titre", location: "Lieu", attendees: "Participants", notes: "Notes" }, noticeCalendarUpdated: "Evenement mis a jour.", noticeCalendarCreated: "Evenement cree.", validationCalendar: "Renseigne date, heure et titre.",
  contactsEditTitle: "Modifier le contact", contactsCreateTitle: "Creer un contact", contactActions: { new: "Nouveau contact", save: "Enregistrer", create: "Creer" }, contactFields: { name: "Nom", role: "Role", email: "Email", phone: "Telephone", team: "Equipe", notes: "Notes" }, noticeContactUpdated: "Contact mis a jour.", noticeContactCreated: "Contact cree.", validationContact: "Renseigne au moins le nom et l'email.",
  settings: {
    delegationLabel: "Delegation", delegationTitle: "Acces mailbox et collections", outgoingSharesTitle: "Partages sortants", incomingSharesTitle: "Collections partagees entrantes", mailboxDelegationTitle: "Delegation mailbox",
    collectionField: "Collection", taskListField: "Liste de taches", granteeEmailField: "Email du delegataire", mailboxGranteeField: "Delegataire mailbox", senderRightField: "Droit d'envoi",
    collectionKinds: { contacts: "Contacts", calendar: "Calendrier", tasks: "Liste de taches" },
    rights: { read: "Lecture", write: "Ecriture", delete: "Suppression", share: "Partage" },
    actions: { saveShare: "Enregistrer le partage", shareInbox: "Partager la boite", grantSenderRight: "Accorder un droit d'envoi" },
    senderRights: { sendAs: "Envoyer comme", sendOnBehalf: "Envoyer au nom de" },
    inboxAccess: "Acces boite de reception", remove: "Retirer", taskListPrefix: "Liste de taches {name}",
    sieveLabel: "Sieve", sieveTitle: "Absence et filtrage", scriptNameField: "Nom du script", activateAfterSaveField: "Activer apres enregistrement", sieveScriptField: "Script Sieve", storedScriptsTitle: "Scripts stockes", bytesLabel: "octets",
    scriptState: { active: "actif", inactive: "inactif" },
    sieveActions: { saveScript: "Enregistrer le script", disableActive: "Desactiver l'actif", load: "Charger", activate: "Activer", delete: "Supprimer" },
    shareUpdated: "Partage mis a jour.", shareRemoved: "Partage retire.", validationTaskList: "Selectionne une liste de taches a partager."
  }
};

const de: Copy = {
  productTitle: "La Poste Electronique", productSubtitle: "Arbeitsbereich fur Mail, Kalender und Kontakte", compose: "Neue Nachricht", sectionLabel: "Primare Navigation", sections: { mail: "Mail", calendar: "Kalender", contacts: "Kontakte", settings: "Einstellungen" }, sectionIcons: { mail: "M", calendar: "K", contacts: "P", settings: "E" },
  loginTitle: "Bei Mail anmelden", loginHelp: "Verwende die vom Administrator konfigurierte Mailbox-Adresse und das Passwort. Fuge den TOTP-Code hinzu, wenn Mailbox-MFA aktiviert ist.", loginEmail: "E-Mail-Adresse", loginPassword: "Passwort", loginTotp: "TOTP-Code", loginSubmit: "Anmelden", loginOidc: "Mit SSO fortfahren", loginOrDivider: "oder Mailbox-SSO verwenden", loginError: "Ungueltige Mailbox-Anmeldedaten.", logout: "Abmelden", signedInAs: "Angemeldet als {email}", accountMenuLabel: "Konto", accountMenuTitle: "Kontomenue",
  shellTabs: ["Datei", "Start", "Ansicht", "Hilfe"], ribbonActions: ["Loschen", "Archivieren", "Melden", "Bereinigen", "Verschieben"], ribbonSecondary: ["Gelesen / Ungelesen", "Markieren", "Anheften", "Drucken"], toolbarChips: ["Auswahl", "Filtern", "Nach Datum"], favoritesLabel: "Favoriten", rightPaneTitle: "Fokussierte Nachricht",
  mailboxLabel: "Ordner", folders: { focused: "Fokussiert", inbox: "Posteingang", drafts: "Entwurfe", sent: "Gesendet", archive: "Archiv" }, workspaceSummary: "Arbeitsbereich-Zusammenfassung", summaryInbox: "Posteingangsstatus", summaryUnread: "{count} ungelesene Unterhaltungen", summaryAgenda: "Heutige Agenda", summaryAgendaCount: "{count} geplante Termine", summaryDrafts: "Entwurfe", summaryDraftsCount: "{count} gespeicherte Entwurfe",
  searchPlaceholder: "Mails, Dateien, Kontakte oder Betreffe suchen", topActions: { sync: "Jetzt synchronisieren" }, languageLabel: "Sprache", heroEyebrow: "Mailbox-Arbeitsbereich", heroTitle: "Aktueller Kontostatus", heroBody: "Mail, Entwurfe, Kontakte und Kalendereintrage stammen direkt aus den kanonischen LPE-Kontodaten.", calendarBody: "Die hier angezeigten Kalendereintrage sind die fur dieses Konto gespeicherten Ereignisse.", contactsBody: "Die hier angezeigten Kontakte sind die fur dieses Konto gespeicherten Personen-Datensatze.",
  metrics: { reliability: "Mail-Zustellung", search: "Suchbereich", searchValue: "Nachrichten + Anhange", attachments: "Indizierte Formate", workflow: "Kernaktionen", workflowValue: "Verfassen, bearbeiten, planen" },
  listColumns: { from: "Von", subject: "Betreff", received: "Empfangen" },
  altViews: { mail: "Mail", calendar: "Agenda", contacts: "Verzeichnis", settings: "Einstellungen" }, messageCount: "{count} Nachrichten", calendarCount: "{count} Ereignisse", contactCount: "{count} Kontakte", noMessages: "Keine Nachrichten entsprechen diesem Ordner und dieser Suche.", noCalendarEvents: "Keine Kalendereintrage fur dieses Konto gespeichert.", noContacts: "Keine Kontakte fur dieses Konto gespeichert.", loadingWorkspace: "Kontodaten werden geladen.", loadError: "Kontodaten konnen nicht geladen werden.", saveError: "Diese Anderung konnte nicht gespeichert werden.", flaggedShort: "Markiert", attachmentCount: "{count} Anhang(e)",
  readingPane: "Lesebereich", drawerTitle: "Detailschublade", messageActions: { reply: "Antworten", forward: "Weiterleiten", archive: "Archivieren" }, attachmentsTitle: "Anhange", attachmentsSubtitle: "Dateien aus der ausgewahlten Unterhaltung", noAttachments: "Keine Anhange in dieser Nachricht.",
  altDetailLabels: { calendar: "Kalendereditor", contacts: "Kontakteditor" }, editorLabel: "Nachrichteneditor", editorTitles: { new: "Nachricht verfassen", draft: "Entwurf bearbeiten", reply: "Auf Nachricht antworten", forward: "Nachricht weiterleiten" }, editorActions: { saveDraft: "Entwurf speichern", deleteDraft: "Entwurf loschen", cancel: "Abbrechen", send: "Senden" },
  fields: { from: "Von", senderMode: "Absendermodus", to: "An", cc: "Cc", subject: "Betreff", body: "Inhalt" }, senderModes: { sendAs: "Als Mailbox senden", sendOnBehalf: "Im Auftrag senden" }, nowLabel: "Jetzt", untitledDraft: "Unbenannter Entwurf", emptyDraftPreview: "Nachrichtenentwurf", tags: { draft: "Entwurf", reply: "Antwort", forward: "Weiterleitung", outgoing: "Ausgehend" },
  noticeSent: "Nachricht gesendet.", noticeDraft: "Entwurf gespeichert.", noticeDraftDeleted: "Entwurf geloscht.", noticeSyncDone: "Mailbox aktualisiert.", validationMessage: "Fulle fur das Senden mindestens Empfanger sowie Betreff oder Inhalt aus.",
  calendarEditTitle: "Ereignis bearbeiten", calendarCreateTitle: "Ereignis erstellen", calendarActions: { new: "Neues Ereignis", save: "Anderungen speichern", create: "Ereignis erstellen" }, calendarFields: { date: "Datum", time: "Uhrzeit", title: "Titel", location: "Ort", attendees: "Teilnehmer", notes: "Notizen" }, noticeCalendarUpdated: "Ereignis aktualisiert.", noticeCalendarCreated: "Ereignis erstellt.", validationCalendar: "Datum, Uhrzeit und Titel ausfullen.",
  contactsEditTitle: "Kontakt bearbeiten", contactsCreateTitle: "Kontakt erstellen", contactActions: { new: "Neuer Kontakt", save: "Anderungen speichern", create: "Kontakt erstellen" }, contactFields: { name: "Name", role: "Rolle", email: "E-Mail", phone: "Telefon", team: "Team", notes: "Notizen" }, noticeContactUpdated: "Kontakt aktualisiert.", noticeContactCreated: "Kontakt erstellt.", validationContact: "Mindestens Name und E-Mail ausfullen.",
  settings: {
    delegationLabel: "Delegation", delegationTitle: "Mailbox- und Sammlungszugriff", outgoingSharesTitle: "Ausgehende Freigaben", incomingSharesTitle: "Eingehende geteilte Sammlungen", mailboxDelegationTitle: "Mailbox-Delegation",
    collectionField: "Sammlung", taskListField: "Aufgabenliste", granteeEmailField: "E-Mail des Empfaengers", mailboxGranteeField: "Mailbox-Empfaenger", senderRightField: "Senderrecht",
    collectionKinds: { contacts: "Kontakte", calendar: "Kalender", tasks: "Aufgabenliste" },
    rights: { read: "Lesen", write: "Schreiben", delete: "Loeschen", share: "Teilen" },
    actions: { saveShare: "Freigabe speichern", shareInbox: "Posteingang teilen", grantSenderRight: "Senderrecht vergeben" },
    senderRights: { sendAs: "Als senden", sendOnBehalf: "Im Auftrag senden" },
    inboxAccess: "Posteingangszugriff", remove: "Entfernen", taskListPrefix: "Aufgabenliste {name}",
    sieveLabel: "Sieve", sieveTitle: "Abwesenheit und Filter", scriptNameField: "Skriptname", activateAfterSaveField: "Nach dem Speichern aktivieren", sieveScriptField: "Sieve-Skript", storedScriptsTitle: "Gespeicherte Skripte", bytesLabel: "Bytes",
    scriptState: { active: "aktiv", inactive: "inaktiv" },
    sieveActions: { saveScript: "Skript speichern", disableActive: "Aktives deaktivieren", load: "Laden", activate: "Aktivieren", delete: "Loeschen" },
    shareUpdated: "Freigabe aktualisiert.", shareRemoved: "Freigabe entfernt.", validationTaskList: "Waehlen Sie eine Aufgabenliste zum Teilen aus."
  }
};

const it: Copy = {
  productTitle: "La Poste Electronique", productSubtitle: "Spazio di lavoro per posta, calendario e contatti", compose: "Nuovo messaggio", sectionLabel: "Navigazione principale", sections: { mail: "Posta", calendar: "Calendario", contacts: "Contatti", settings: "Impostazioni" }, sectionIcons: { mail: "P", calendar: "C", contacts: "P", settings: "I" },
  loginTitle: "Accedi alla posta", loginHelp: "Usa l'indirizzo della casella e la password configurati dall'amministratore. Aggiungi il codice TOTP quando la MFA della casella e attiva.", loginEmail: "Indirizzo email", loginPassword: "Password", loginTotp: "Codice TOTP", loginSubmit: "Accedi", loginOidc: "Continua con SSO", loginOrDivider: "oppure usa SSO mailbox", loginError: "Credenziali mailbox non valide.", logout: "Disconnetti", signedInAs: "Connesso come {email}", accountMenuLabel: "Account", accountMenuTitle: "Menu account",
  shellTabs: ["File", "Home", "Vista", "Aiuto"], ribbonActions: ["Elimina", "Archivia", "Segnala", "Pulisci", "Sposta"], ribbonSecondary: ["Letto / Non letto", "Contrassegna", "Fissa", "Stampa"], toolbarChips: ["Selezione", "Filtra", "Per data"], favoritesLabel: "Preferiti", rightPaneTitle: "Messaggio in evidenza",
  mailboxLabel: "Cartelle", folders: { focused: "In evidenza", inbox: "Posta in arrivo", drafts: "Bozze", sent: "Inviati", archive: "Archivio" }, workspaceSummary: "Riepilogo spazio di lavoro", summaryInbox: "Stato della posta in arrivo", summaryUnread: "{count} conversazioni non lette", summaryAgenda: "Agenda di oggi", summaryAgendaCount: "{count} eventi pianificati", summaryDrafts: "Bozze", summaryDraftsCount: "{count} bozze salvate",
  searchPlaceholder: "Cerca posta, file, contatti o oggetti", topActions: { sync: "Sincronizza ora" }, languageLabel: "Lingua", heroEyebrow: "Spazio mailbox", heroTitle: "Stato attuale dell'account", heroBody: "Posta, bozze, contatti ed eventi del calendario provengono direttamente dai dati canonici dell'account LPE.", calendarBody: "Gli eventi mostrati qui sono quelli persistenti per questo account.", contactsBody: "I contatti mostrati qui sono i record persone persistenti per questo account.",
  metrics: { reliability: "Consegna posta", search: "Ambito di ricerca", searchValue: "Messaggi + allegati", attachments: "Formati indicizzati", workflow: "Azioni principali", workflowValue: "Componi, modifica, pianifica" },
  listColumns: { from: "Da", subject: "Oggetto", received: "Ricevuto" },
  altViews: { mail: "Posta", calendar: "Agenda", contacts: "Rubrica", settings: "Impostazioni" }, messageCount: "{count} messaggi", calendarCount: "{count} eventi", contactCount: "{count} contatti", noMessages: "Nessun messaggio corrisponde a questa cartella e a questa ricerca.", noCalendarEvents: "Nessun evento di calendario salvato per questo account.", noContacts: "Nessun contatto salvato per questo account.", loadingWorkspace: "Caricamento dati account.", loadError: "Impossibile caricare i dati dell'account.", saveError: "Impossibile salvare questa modifica.", flaggedShort: "Contrassegnato", attachmentCount: "{count} allegato/i",
  readingPane: "Pannello di lettura", drawerTitle: "Pannello dettagli", messageActions: { reply: "Rispondi", forward: "Inoltra", archive: "Archivia" }, attachmentsTitle: "Allegati", attachmentsSubtitle: "File visibili dalla conversazione selezionata", noAttachments: "Nessun allegato in questo messaggio.",
  altDetailLabels: { calendar: "Editor calendario", contacts: "Editor contatti" }, editorLabel: "Editor messaggio", editorTitles: { new: "Componi messaggio", draft: "Modifica bozza", reply: "Rispondi al messaggio", forward: "Inoltra messaggio" }, editorActions: { saveDraft: "Salva bozza", deleteDraft: "Elimina bozza", cancel: "Annulla", send: "Invia" },
  fields: { from: "Da", senderMode: "Modalita mittente", to: "A", cc: "Cc", subject: "Oggetto", body: "Corpo" }, senderModes: { sendAs: "Invia come mailbox", sendOnBehalf: "Invia per conto di" }, nowLabel: "Ora", untitledDraft: "Bozza senza titolo", emptyDraftPreview: "Messaggio in bozza", tags: { draft: "Bozza", reply: "Risposta", forward: "Inoltro", outgoing: "In uscita" },
  noticeSent: "Messaggio inviato.", noticeDraft: "Bozza salvata.", noticeDraftDeleted: "Bozza eliminata.", noticeSyncDone: "Mailbox aggiornata.", validationMessage: "Compila almeno il destinatario per l'invio e un oggetto o del contenuto.",
  calendarEditTitle: "Modifica evento", calendarCreateTitle: "Crea evento", calendarActions: { new: "Nuovo evento", save: "Salva modifiche", create: "Crea evento" }, calendarFields: { date: "Data", time: "Ora", title: "Titolo", location: "Luogo", attendees: "Partecipanti", notes: "Note" }, noticeCalendarUpdated: "Evento aggiornato.", noticeCalendarCreated: "Evento creato.", validationCalendar: "Compila data, ora e titolo.",
  contactsEditTitle: "Modifica contatto", contactsCreateTitle: "Crea contatto", contactActions: { new: "Nuovo contatto", save: "Salva modifiche", create: "Crea contatto" }, contactFields: { name: "Nome", role: "Ruolo", email: "Email", phone: "Telefono", team: "Team", notes: "Note" }, noticeContactUpdated: "Contatto aggiornato.", noticeContactCreated: "Contatto creato.", validationContact: "Compila almeno nome ed email.",
  settings: {
    delegationLabel: "Delega", delegationTitle: "Accesso a mailbox e raccolte", outgoingSharesTitle: "Condivisioni in uscita", incomingSharesTitle: "Raccolte condivise in ingresso", mailboxDelegationTitle: "Delega mailbox",
    collectionField: "Raccolta", taskListField: "Lista attivita", granteeEmailField: "Email destinatario", mailboxGranteeField: "Destinatario mailbox", senderRightField: "Diritto mittente",
    collectionKinds: { contacts: "Contatti", calendar: "Calendario", tasks: "Lista attivita" },
    rights: { read: "Lettura", write: "Scrittura", delete: "Eliminazione", share: "Condivisione" },
    actions: { saveShare: "Salva condivisione", shareInbox: "Condividi inbox", grantSenderRight: "Concedi diritto mittente" },
    senderRights: { sendAs: "Invia come", sendOnBehalf: "Invia per conto di" },
    inboxAccess: "Accesso inbox", remove: "Rimuovi", taskListPrefix: "Lista attivita {name}",
    sieveLabel: "Sieve", sieveTitle: "Assenza e filtri", scriptNameField: "Nome script", activateAfterSaveField: "Attiva dopo il salvataggio", sieveScriptField: "Script Sieve", storedScriptsTitle: "Script salvati", bytesLabel: "byte",
    scriptState: { active: "attivo", inactive: "inattivo" },
    sieveActions: { saveScript: "Salva script", disableActive: "Disattiva attivo", load: "Carica", activate: "Attiva", delete: "Elimina" },
    shareUpdated: "Condivisione aggiornata.", shareRemoved: "Condivisione rimossa.", validationTaskList: "Seleziona una lista attivita da condividere."
  }
};

const es: Copy = {
  productTitle: "La Poste Electronique", productSubtitle: "Espacio de trabajo para correo, calendario y contactos", compose: "Nuevo mensaje", sectionLabel: "Navegacion principal", sections: { mail: "Correo", calendar: "Calendario", contacts: "Contactos", settings: "Configuracion" }, sectionIcons: { mail: "C", calendar: "C", contacts: "P", settings: "A" },
  loginTitle: "Iniciar sesion en correo", loginHelp: "Usa la direccion del buzon y la contrasena configuradas por el administrador. Agrega el codigo TOTP cuando la MFA del buzon este activada.", loginEmail: "Direccion de correo", loginPassword: "Contrasena", loginTotp: "Codigo TOTP", loginSubmit: "Iniciar sesion", loginOidc: "Continuar con SSO", loginOrDivider: "o usar SSO del buzon", loginError: "Credenciales de mailbox no validas.", logout: "Cerrar sesion", signedInAs: "Conectado como {email}", accountMenuLabel: "Cuenta", accountMenuTitle: "Menu de cuenta",
  shellTabs: ["Archivo", "Inicio", "Vista", "Ayuda"], ribbonActions: ["Eliminar", "Archivar", "Reportar", "Limpiar", "Mover"], ribbonSecondary: ["Leido / No leido", "Marcar", "Fijar", "Imprimir"], toolbarChips: ["Seleccion", "Filtrar", "Por fecha"], favoritesLabel: "Favoritos", rightPaneTitle: "Mensaje destacado",
  mailboxLabel: "Carpetas", folders: { focused: "Destacado", inbox: "Bandeja de entrada", drafts: "Borradores", sent: "Enviados", archive: "Archivo" }, workspaceSummary: "Resumen del espacio de trabajo", summaryInbox: "Estado de la bandeja", summaryUnread: "{count} conversaciones no leidas", summaryAgenda: "Agenda de hoy", summaryAgendaCount: "{count} eventos programados", summaryDrafts: "Borradores", summaryDraftsCount: "{count} borradores guardados",
  searchPlaceholder: "Buscar correo, archivos, contactos o asuntos", topActions: { sync: "Sincronizar ahora" }, languageLabel: "Idioma", heroEyebrow: "Espacio mailbox", heroTitle: "Estado actual de la cuenta", heroBody: "Correo, borradores, contactos y entradas de calendario provienen directamente de los datos canonicos de la cuenta LPE.", calendarBody: "Los eventos mostrados aqui son los eventos persistidos para esta cuenta.", contactsBody: "Los contactos mostrados aqui son los registros persistidos de personas para esta cuenta.",
  metrics: { reliability: "Entrega de correo", search: "Alcance de busqueda", searchValue: "Mensajes + adjuntos", attachments: "Formatos indexados", workflow: "Acciones principales", workflowValue: "Redactar, editar, planificar" },
  listColumns: { from: "De", subject: "Asunto", received: "Recibido" },
  altViews: { mail: "Correo", calendar: "Agenda", contacts: "Directorio", settings: "Configuracion" }, messageCount: "{count} mensajes", calendarCount: "{count} eventos", contactCount: "{count} contactos", noMessages: "Ningun mensaje coincide con esta carpeta y esta busqueda.", noCalendarEvents: "No hay eventos de calendario almacenados para esta cuenta.", noContacts: "No hay contactos almacenados para esta cuenta.", loadingWorkspace: "Cargando datos de la cuenta.", loadError: "No se pueden cargar los datos de la cuenta.", saveError: "No se puede guardar este cambio.", flaggedShort: "Marcado", attachmentCount: "{count} adjunto(s)",
  readingPane: "Panel de lectura", drawerTitle: "Panel de detalles", messageActions: { reply: "Responder", forward: "Reenviar", archive: "Archivar" }, attachmentsTitle: "Adjuntos", attachmentsSubtitle: "Archivos visibles de la conversacion seleccionada", noAttachments: "No hay adjuntos en este mensaje.",
  altDetailLabels: { calendar: "Editor de calendario", contacts: "Editor de contactos" }, editorLabel: "Editor de mensajes", editorTitles: { new: "Redactar mensaje", draft: "Editar borrador", reply: "Responder al mensaje", forward: "Reenviar mensaje" }, editorActions: { saveDraft: "Guardar borrador", deleteDraft: "Eliminar borrador", cancel: "Cancelar", send: "Enviar" },
  fields: { from: "De", senderMode: "Modo del remitente", to: "Para", cc: "Cc", subject: "Asunto", body: "Cuerpo" }, senderModes: { sendAs: "Enviar como mailbox", sendOnBehalf: "Enviar en nombre de" }, nowLabel: "Ahora", untitledDraft: "Borrador sin titulo", emptyDraftPreview: "Mensaje en borrador", tags: { draft: "Borrador", reply: "Respuesta", forward: "Reenvio", outgoing: "Saliente" },
  noticeSent: "Mensaje enviado.", noticeDraft: "Borrador guardado.", noticeDraftDeleted: "Borrador eliminado.", noticeSyncDone: "Mailbox actualizada.", validationMessage: "Completa al menos el destinatario para enviar y algun asunto o contenido.",
  calendarEditTitle: "Editar evento", calendarCreateTitle: "Crear evento", calendarActions: { new: "Nuevo evento", save: "Guardar cambios", create: "Crear evento" }, calendarFields: { date: "Fecha", time: "Hora", title: "Titulo", location: "Ubicacion", attendees: "Asistentes", notes: "Notas" }, noticeCalendarUpdated: "Evento actualizado.", noticeCalendarCreated: "Evento creado.", validationCalendar: "Completa fecha, hora y titulo.",
  contactsEditTitle: "Editar contacto", contactsCreateTitle: "Crear contacto", contactActions: { new: "Nuevo contacto", save: "Guardar cambios", create: "Crear contacto" }, contactFields: { name: "Nombre", role: "Rol", email: "Correo", phone: "Telefono", team: "Equipo", notes: "Notas" }, noticeContactUpdated: "Contacto actualizado.", noticeContactCreated: "Contacto creado.", validationContact: "Completa al menos nombre y correo.",
  settings: {
    delegationLabel: "Delegacion", delegationTitle: "Acceso a mailbox y colecciones", outgoingSharesTitle: "Compartidos salientes", incomingSharesTitle: "Colecciones compartidas entrantes", mailboxDelegationTitle: "Delegacion de mailbox",
    collectionField: "Coleccion", taskListField: "Lista de tareas", granteeEmailField: "Correo del destinatario", mailboxGranteeField: "Destinatario de mailbox", senderRightField: "Permiso de remitente",
    collectionKinds: { contacts: "Contactos", calendar: "Calendario", tasks: "Lista de tareas" },
    rights: { read: "Lectura", write: "Escritura", delete: "Eliminacion", share: "Compartir" },
    actions: { saveShare: "Guardar compartido", shareInbox: "Compartir inbox", grantSenderRight: "Conceder permiso de remitente" },
    senderRights: { sendAs: "Enviar como", sendOnBehalf: "Enviar en nombre de" },
    inboxAccess: "Acceso a inbox", remove: "Eliminar", taskListPrefix: "Lista de tareas {name}",
    sieveLabel: "Sieve", sieveTitle: "Ausencia y filtrado", scriptNameField: "Nombre del script", activateAfterSaveField: "Activar despues de guardar", sieveScriptField: "Script Sieve", storedScriptsTitle: "Scripts guardados", bytesLabel: "bytes",
    scriptState: { active: "activo", inactive: "inactivo" },
    sieveActions: { saveScript: "Guardar script", disableActive: "Desactivar activo", load: "Cargar", activate: "Activar", delete: "Eliminar" },
    shareUpdated: "Compartido actualizado.", shareRemoved: "Compartido eliminado.", validationTaskList: "Selecciona una lista de tareas para compartir."
  }
};

export const messages = defineLocaleCatalog(en, { fr, de, it, es });
