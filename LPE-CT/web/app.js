const feedback = document.getElementById("feedback");
const loginFeedback = document.getElementById("login-feedback");
const loginShell = document.getElementById("login-shell");
const consoleShell = document.getElementById("console-shell");
const configDrawer = document.getElementById("config-drawer");
const drawerTitle = document.getElementById("drawer-title");
const drawerSummary = document.getElementById("drawer-summary");
const drawerPanels = Array.from(document.querySelectorAll(".drawer-panel"));
const panelTriggers = Array.from(document.querySelectorAll("[data-open-panel]"));
const localePickers = Array.from(document.querySelectorAll("[data-locale-picker]"));
const AUTH_TOKEN_KEY = "lpeCtAdminToken";
const LAST_ADMIN_EMAIL_KEY = "lpeCtAdminLastEmail";
const LOCALE_KEY = "lpe.locale";
const supportedLocales = ["en", "fr", "de", "it", "es"];
const localeLabels = { en: "English", fr: "Francais", de: "Deutsch", it: "Italiano", es: "Espanol" };

const messages = {
  en: {
    pageTitle: "LPE-CT Management Console",
    languageLabel: "Language",
    brand: "La Poste Electronique",
    loginTitle: "LPE-CT Management Login",
    loginCopy:
      "First access uses the bootstrap administrator. Change the bootstrap secret in deployment configuration after initial validation.",
    adminEmail: "Admin email",
    password: "Password",
    signIn: "Sign in",
    workspace: "Workspace",
    governance: "Governance",
    consoleTitle: "Sorting Center",
    refresh: "Refresh",
    heroKicker: "DMZ sorting center",
    heroLoadingTitle: "Loading...",
    heroLoadingSummary: "Reading sorting-center state.",
    consoleIntro:
      "Management interface for a DMZ node distinct from the LAN, focused on routing, quarantine, inbound policies, and updates.",
    refreshState: "Refresh state",
    policiesShort: "Policies",
    relayShort: "Relay",
    pillBoundary: "DMZ boundary",
    pillMagika: "Magika aware",
    pillQueue: "Queue discipline",
    inbound: "Inbound",
    deferred: "Deferred",
    quarantine: "Quarantine",
    attemptsPerHour: "Attempts / h",
    configTitle: "Sorting-center configuration",
    configSummary: "Select a section to open its management drawer.",
    quickActions: "Quick actions",
    quickActionsCopy: "Frequent operations for the DMZ node.",
    reviewIdentity: "Review identity",
    reviewNetwork: "Review network",
    planUpdate: "Plan update",
    quarantineFocus: "Quarantine focus",
    quarantineFocusCopy: "Items that should remain one click away for perimeter review.",
    quarantineItemOneTitle: "invoice-0427.zip",
    quarantineItemOneCopy: "Detected as exotic payload by Magika. Held in LPE-CT.",
    quarantineItemTwoTitle: "board-minutes.p7m",
    quarantineItemTwoCopy: "Encrypted payload marked uninspectable. Awaiting policy review.",
    pendingReview: "Pending review",
    encrypted: "Encrypted",
    drawerConfig: "Configuration",
    drawerSummaryDefault: "Edit the selected section.",
    close: "Close",
    saveSite: "Save profile",
    saveRelay: "Save relay",
    saveNetwork: "Save network",
    savePolicies: "Save policies",
    saveUpdates: "Save updates",
    auditTitle: "Recent journal",
    auditCopy: "Short trace of sorting-center configuration changes.",
    drawerSiteTitle: "Node identity",
    drawerSiteSummary: "Positioning and exposure of the sorting server in the DMZ.",
    drawerRelayTitle: "Relay toward LAN",
    drawerRelaySummary: "Strictly allowed flows between the DMZ and the LPE core.",
    drawerNetworkTitle: "Network surface",
    drawerNetworkSummary: "Restriction of management CIDRs, egress paths, and public listeners.",
    drawerPoliciesTitle: "Sorting policies",
    drawerPoliciesSummary: "Control of drain mode, quarantine, and verification requirements.",
    drawerUpdatesTitle: "Updates",
    drawerUpdatesSummary: "Git-first Debian update policy for the DMZ node.",
    navSiteTitle: "Node identity",
    navRelayTitle: "Relay toward LAN",
    navNetworkTitle: "Network surface",
    navPoliciesTitle: "Sorting policies",
    navUpdatesTitle: "Updates",
    navSiteCopy: "Positioning and exposure of the sorting server in the DMZ.",
    navRelayCopy: "Strictly allowed flows between the DMZ and the LPE core.",
    navNetworkCopy: "Restriction of management CIDRs, egress paths, and public listeners.",
    navPoliciesCopy: "Control of drain mode, quarantine, and verification requirements.",
    navUpdatesCopy: "Git-first Debian update policy for the DMZ node.",
    managementSiteTitle: "Node identity",
    managementSiteCopy: "Name, role, region, FQDN, and published interfaces.",
    managementSitePill: "DMZ profile",
    managementRelayTitle: "Relay toward LAN",
    managementRelayCopy: "Upstreams, mTLS, hold queue, and LAN dependencies.",
    managementRelayPill: "Transport",
    managementNetworkTitle: "Network surface",
    managementNetworkCopy: "Allowed CIDRs, smart hosts, listeners, and proxy protocol.",
    managementNetworkPill: "DMZ",
    managementPoliciesTitle: "Sorting policies",
    managementPoliciesCopy: "SPF, DKIM, DMARC, greylisting, quarantine, and message size.",
    managementPoliciesPill: "Security",
    managementUpdatesTitle: "Updates",
    managementUpdatesCopy: "Channel, maintenance window, Git source, and download policy.",
    managementUpdatesPill: "Operations",
    open: "Open",
    nodeName: "Node name",
    role: "Role",
    region: "Region",
    dmzZone: "DMZ zone",
    publishedMx: "Published MX",
    managementFqdn: "Management FQDN",
    publicSmtpBind: "Public SMTP bind",
    managementBind: "Management bind",
    primaryUpstream: "Primary upstream",
    secondaryUpstream: "Secondary upstream",
    syncInterval: "Sync interval (s)",
    lanDependencyNote: "LAN dependency note",
    mutualTlsRequired: "Mutual TLS required",
    fallbackToHoldQueue: "Fallback to hold queue",
    allowedManagementCidrs: "Allowed management CIDRs",
    allowedUpstreamCidrs: "Allowed upstream CIDRs",
    outboundSmartHosts: "Outbound smart hosts",
    maxConcurrentSessions: "Max concurrent sessions",
    publicListenerEnabled: "Public listener enabled",
    submissionListenerEnabled: "Submission listener enabled",
    proxyProtocolEnabled: "Proxy protocol enabled",
    maxMessageSizeMb: "Max message size (MB)",
    drainMode: "Drain mode",
    quarantineEnabled: "Quarantine enabled",
    greylistingEnabled: "Greylisting enabled",
    requireSpf: "Require SPF",
    requireDkimAlignment: "Require DKIM alignment",
    requireDmarcEnforcement: "Require DMARC enforcement",
    attachmentTextScanEnabled: "Attachment text scan enabled",
    channel: "Channel",
    maintenanceWindow: "Maintenance window",
    lastAppliedRelease: "Last applied release",
    updateSource: "Update source",
    autoDownload: "Auto download",
    authRequired: "Management authentication required.",
    unknownError: "Unknown error.",
    login502: "Management API unreachable (502). Check lpe-ct.service and nginx.",
    authenticated: "Authenticated.",
    savedSite: "DMZ profile saved.",
    savedRelay: "LAN relay saved.",
    savedNetwork: "Network surface saved.",
    savedPolicies: "Sorting policies saved.",
    savedUpdates: "Update policy saved.",
    statusDrain: "Drain mode",
    statusProduction: "Production",
    relayReachable: "LAN relay reachable",
    relayUnreachable: "LAN relay unreachable",
    heroSummaryTemplate: "{dmzZone} · MX {publishedMx} · primary relay {primaryUpstream}",
  },
  fr: {
    pageTitle: "Console de management LPE-CT",
    languageLabel: "Langue",
    brand: "La Poste Electronique",
    loginTitle: "Connexion management LPE-CT",
    loginCopy:
      "Le premier acces utilise l'administrateur bootstrap. Changez le secret bootstrap dans la configuration de deploiement apres la validation initiale.",
    adminEmail: "Email administrateur",
    password: "Mot de passe",
    signIn: "Se connecter",
    workspace: "Espace",
    governance: "Gouvernance",
    consoleTitle: "Centre de Tri",
    refresh: "Actualiser",
    heroKicker: "Centre de tri DMZ",
    heroLoadingTitle: "Chargement...",
    heroLoadingSummary: "Lecture de l'etat du centre de tri.",
    consoleIntro:
      "Interface de management pour un noeud DMZ distinct du LAN, focalisee sur le routage, la quarantaine, les politiques d'entree et les mises a jour.",
    refreshState: "Actualiser l'etat",
    policiesShort: "Politiques",
    relayShort: "Relais",
    pillBoundary: "Frontiere DMZ",
    pillMagika: "Compatible Magika",
    pillQueue: "Discipline de file",
    inbound: "Entrant",
    deferred: "Differe",
    quarantine: "Quarantaine",
    attemptsPerHour: "Tentatives / h",
    configTitle: "Configuration du centre de tri",
    configSummary: "Selectionner une section pour ouvrir son panneau de gestion.",
    quickActions: "Actions rapides",
    quickActionsCopy: "Operations frequentes pour le noeud DMZ.",
    reviewIdentity: "Verifier l'identite",
    reviewNetwork: "Verifier le reseau",
    planUpdate: "Planifier la mise a jour",
    quarantineFocus: "Focus quarantaine",
    quarantineFocusCopy: "Elements qui doivent rester accessibles en un clic pour la revue perimetrique.",
    quarantineItemOneTitle: "invoice-0427.zip",
    quarantineItemOneCopy: "Detecte comme charge utile exotique par Magika. Retenu dans LPE-CT.",
    quarantineItemTwoTitle: "board-minutes.p7m",
    quarantineItemTwoCopy: "Charge chiffre evaluee comme non inspectable. En attente de revue de politique.",
    pendingReview: "Revue en attente",
    encrypted: "Chiffre",
    drawerConfig: "Configuration",
    drawerSummaryDefault: "Modifier la section selectionnee.",
    close: "Fermer",
    saveSite: "Enregistrer le profil",
    saveRelay: "Enregistrer le relais",
    saveNetwork: "Enregistrer le reseau",
    savePolicies: "Enregistrer les politiques",
    saveUpdates: "Enregistrer les mises a jour",
    auditTitle: "Journal recent",
    auditCopy: "Trace courte des changements de configuration du centre de tri.",
    drawerSiteTitle: "Identite du noeud",
    drawerSiteSummary: "Positionnement et exposition du serveur de tri en DMZ.",
    drawerRelayTitle: "Relais vers le LAN",
    drawerRelaySummary: "Flux strictement autorises entre la DMZ et le coeur LPE.",
    drawerNetworkTitle: "Surface reseau",
    drawerNetworkSummary: "Restriction des CIDR de management, sorties et listeners publics.",
    drawerPoliciesTitle: "Politiques de tri",
    drawerPoliciesSummary: "Controle du drain mode, quarantaine et exigences de verification.",
    drawerUpdatesTitle: "Mises a jour",
    drawerUpdatesSummary: "Politique de mise a jour Debian Git-first pour le noeud DMZ.",
    navSiteTitle: "Identite du noeud",
    navRelayTitle: "Relais vers le LAN",
    navNetworkTitle: "Surface reseau",
    navPoliciesTitle: "Politiques de tri",
    navUpdatesTitle: "Mises a jour",
    navSiteCopy: "Positionnement et exposition du serveur de tri en DMZ.",
    navRelayCopy: "Flux strictement autorises entre la DMZ et le coeur LPE.",
    navNetworkCopy: "Restriction des CIDR de management, sorties et listeners publics.",
    navPoliciesCopy: "Controle du drain mode, quarantaine et exigences de verification.",
    navUpdatesCopy: "Politique de mise a jour Debian Git-first pour le noeud DMZ.",
    managementSiteTitle: "Identite du noeud",
    managementSiteCopy: "Nom, role, region, FQDN et interfaces publiees.",
    managementSitePill: "Profil DMZ",
    managementRelayTitle: "Relais vers le LAN",
    managementRelayCopy: "Upstreams, mTLS, hold queue et dependances LAN.",
    managementRelayPill: "Transport",
    managementNetworkTitle: "Surface reseau",
    managementNetworkCopy: "CIDR autorises, smart hosts, listeners et proxy protocol.",
    managementNetworkPill: "DMZ",
    managementPoliciesTitle: "Politiques de tri",
    managementPoliciesCopy: "SPF, DKIM, DMARC, greylisting, quarantaine et taille message.",
    managementPoliciesPill: "Securite",
    managementUpdatesTitle: "Mises a jour",
    managementUpdatesCopy: "Canal, fenetre de maintenance, source Git et telechargement.",
    managementUpdatesPill: "Operations",
    open: "Ouvrir",
    nodeName: "Nom du noeud",
    role: "Role",
    region: "Region",
    dmzZone: "Zone DMZ",
    publishedMx: "MX publie",
    managementFqdn: "FQDN management",
    publicSmtpBind: "Bind SMTP public",
    managementBind: "Bind management",
    primaryUpstream: "Upstream principal",
    secondaryUpstream: "Upstream secondaire",
    syncInterval: "Intervalle de synchro (s)",
    lanDependencyNote: "Note de dependance LAN",
    mutualTlsRequired: "mTLS requis",
    fallbackToHoldQueue: "Basculer vers la hold queue",
    allowedManagementCidrs: "CIDR management autorises",
    allowedUpstreamCidrs: "CIDR upstream autorises",
    outboundSmartHosts: "Smart hosts sortants",
    maxConcurrentSessions: "Sessions concurrentes max",
    publicListenerEnabled: "Listener public active",
    submissionListenerEnabled: "Listener submission active",
    proxyProtocolEnabled: "Proxy protocol active",
    maxMessageSizeMb: "Taille max message (Mo)",
    drainMode: "Drain mode",
    quarantineEnabled: "Quarantaine active",
    greylistingEnabled: "Greylisting actif",
    requireSpf: "Exiger SPF",
    requireDkimAlignment: "Exiger l'alignement DKIM",
    requireDmarcEnforcement: "Exiger l'application DMARC",
    attachmentTextScanEnabled: "Analyse texte des pieces jointees activee",
    channel: "Canal",
    maintenanceWindow: "Fenetre de maintenance",
    lastAppliedRelease: "Derniere release appliquee",
    updateSource: "Source de mise a jour",
    autoDownload: "Telechargement auto",
    authRequired: "Authentification management requise.",
    unknownError: "Erreur inconnue.",
    login502: "API de management inaccessible (502). Verifier lpe-ct.service et nginx.",
    authenticated: "Authentifie.",
    savedSite: "Profil DMZ enregistre.",
    savedRelay: "Relais LAN enregistre.",
    savedNetwork: "Surface reseau enregistree.",
    savedPolicies: "Politiques de tri enregistrees.",
    savedUpdates: "Politique de mise a jour enregistree.",
    statusDrain: "Drain mode",
    statusProduction: "Production",
    relayReachable: "Relais LAN joignable",
    relayUnreachable: "Relais LAN injoignable",
    heroSummaryTemplate: "{dmzZone} · MX {publishedMx} · relais primaire {primaryUpstream}",
  },
  de: {
    pageTitle: "LPE-CT Verwaltungsoberflaeche",
    languageLabel: "Sprache",
    brand: "La Poste Electronique",
    loginTitle: "LPE-CT Verwaltungsanmeldung",
    loginCopy:
      "Der erste Zugriff verwendet den Bootstrap-Administrator. Aendern Sie das Bootstrap-Geheimnis in der Bereitstellungskonfiguration nach der ersten Validierung.",
    adminEmail: "Administrator-E-Mail",
    password: "Passwort",
    signIn: "Anmelden",
    workspace: "Arbeitsbereich",
    governance: "Governance",
    consoleTitle: "Sortierzentrum",
    refresh: "Aktualisieren",
    heroKicker: "DMZ-Sortierzentrum",
    heroLoadingTitle: "Wird geladen...",
    heroLoadingSummary: "Status des Sortierzentrums wird geladen.",
    consoleIntro:
      "Verwaltungsoberflaeche fuer einen von der LAN-Zone getrennten DMZ-Knoten mit Fokus auf Routing, Quarantaene, Eingangsrichtlinien und Updates.",
    refreshState: "Status aktualisieren",
    policiesShort: "Richtlinien",
    relayShort: "Relay",
    pillBoundary: "DMZ-Grenze",
    pillMagika: "Magika-faehig",
    pillQueue: "Queue-Disziplin",
    inbound: "Eingehend",
    deferred: "Zurueckgestellt",
    quarantine: "Quarantaene",
    attemptsPerHour: "Versuche / h",
    configTitle: "Konfiguration des Sortierzentrums",
    configSummary: "Waehlen Sie einen Bereich, um den Verwaltungs-Drawer zu oeffnen.",
    quickActions: "Schnellaktionen",
    quickActionsCopy: "Hauefige Vorgaenge fuer den DMZ-Knoten.",
    reviewIdentity: "Identitaet pruefen",
    reviewNetwork: "Netzwerk pruefen",
    planUpdate: "Update planen",
    quarantineFocus: "Quarantaene-Fokus",
    quarantineFocusCopy: "Elemente, die fuer die Perimeter-Pruefung mit einem Klick erreichbar bleiben sollen.",
    quarantineItemOneTitle: "invoice-0427.zip",
    quarantineItemOneCopy: "Von Magika als exotische Nutzlast erkannt. In LPE-CT angehalten.",
    quarantineItemTwoTitle: "board-minutes.p7m",
    quarantineItemTwoCopy: "Verschluesselte Nutzlast als nicht pruefbar markiert. Wartet auf Richtlinienpruefung.",
    pendingReview: "Pruefung ausstehend",
    encrypted: "Verschluesselt",
    drawerConfig: "Konfiguration",
    drawerSummaryDefault: "Ausgewaehlten Bereich bearbeiten.",
    close: "Schliessen",
    saveSite: "Profil speichern",
    saveRelay: "Relay speichern",
    saveNetwork: "Netzwerk speichern",
    savePolicies: "Richtlinien speichern",
    saveUpdates: "Updates speichern",
    auditTitle: "Letztes Journal",
    auditCopy: "Kurze Spur der Konfigurationsaenderungen des Sortierzentrums.",
    drawerSiteTitle: "Knotenidentitaet",
    drawerSiteSummary: "Positionierung und Sichtbarkeit des Sortierservers in der DMZ.",
    drawerRelayTitle: "Relay zum LAN",
    drawerRelaySummary: "Strikt erlaubte Fluesse zwischen der DMZ und dem LPE-Kern.",
    drawerNetworkTitle: "Netzwerkoberflaeche",
    drawerNetworkSummary: "Einschraenkung von Management-CIDRs, Ausgaengen und oeffentlichen Listenern.",
    drawerPoliciesTitle: "Sortierrichtlinien",
    drawerPoliciesSummary: "Steuerung von Drain-Modus, Quarantaene und Pruefanforderungen.",
    drawerUpdatesTitle: "Updates",
    drawerUpdatesSummary: "Git-first-Debian-Updatepolitik fuer den DMZ-Knoten.",
    navSiteTitle: "Knotenidentitaet",
    navRelayTitle: "Relay zum LAN",
    navNetworkTitle: "Netzwerkoberflaeche",
    navPoliciesTitle: "Sortierrichtlinien",
    navUpdatesTitle: "Updates",
    navSiteCopy: "Positionierung und Sichtbarkeit des Sortierservers in der DMZ.",
    navRelayCopy: "Strikt erlaubte Fluesse zwischen der DMZ und dem LPE-Kern.",
    navNetworkCopy: "Einschraenkung von Management-CIDRs, Ausgaengen und oeffentlichen Listenern.",
    navPoliciesCopy: "Steuerung von Drain-Modus, Quarantaene und Pruefanforderungen.",
    navUpdatesCopy: "Git-first-Debian-Updatepolitik fuer den DMZ-Knoten.",
    managementSiteTitle: "Knotenidentitaet",
    managementSiteCopy: "Name, Rolle, Region, FQDN und veroeffentlichte Schnittstellen.",
    managementSitePill: "DMZ-Profil",
    managementRelayTitle: "Relay zum LAN",
    managementRelayCopy: "Upstreams, mTLS, Hold Queue und LAN-Abhaengigkeiten.",
    managementRelayPill: "Transport",
    managementNetworkTitle: "Netzwerkoberflaeche",
    managementNetworkCopy: "Erlaubte CIDRs, Smart Hosts, Listener und Proxy Protocol.",
    managementNetworkPill: "DMZ",
    managementPoliciesTitle: "Sortierrichtlinien",
    managementPoliciesCopy: "SPF, DKIM, DMARC, Greylisting, Quarantaene und Nachrichtengroesse.",
    managementPoliciesPill: "Sicherheit",
    managementUpdatesTitle: "Updates",
    managementUpdatesCopy: "Kanal, Wartungsfenster, Git-Quelle und Downloadpolitik.",
    managementUpdatesPill: "Betrieb",
    open: "Oeffnen",
    nodeName: "Knotenname",
    role: "Rolle",
    region: "Region",
    dmzZone: "DMZ-Zone",
    publishedMx: "Veroeffentlichtes MX",
    managementFqdn: "Management-FQDN",
    publicSmtpBind: "Oeffentlicher SMTP-Bind",
    managementBind: "Management-Bind",
    primaryUpstream: "Primaerer Upstream",
    secondaryUpstream: "Sekundaerer Upstream",
    syncInterval: "Sync-Intervall (s)",
    lanDependencyNote: "Hinweis zur LAN-Abhaengigkeit",
    mutualTlsRequired: "mTLS erforderlich",
    fallbackToHoldQueue: "Auf Hold Queue zurueckfallen",
    allowedManagementCidrs: "Erlaubte Management-CIDRs",
    allowedUpstreamCidrs: "Erlaubte Upstream-CIDRs",
    outboundSmartHosts: "Ausgehende Smart Hosts",
    maxConcurrentSessions: "Max. gleichzeitige Sitzungen",
    publicListenerEnabled: "Oeffentlicher Listener aktiv",
    submissionListenerEnabled: "Submission-Listener aktiv",
    proxyProtocolEnabled: "Proxy Protocol aktiv",
    maxMessageSizeMb: "Max. Nachrichtengroesse (MB)",
    drainMode: "Drain-Modus",
    quarantineEnabled: "Quarantaene aktiviert",
    greylistingEnabled: "Greylisting aktiviert",
    requireSpf: "SPF verlangen",
    requireDkimAlignment: "DKIM-Ausrichtung verlangen",
    requireDmarcEnforcement: "DMARC-Durchsetzung verlangen",
    attachmentTextScanEnabled: "Textscan fuer Anhaenge aktiviert",
    channel: "Kanal",
    maintenanceWindow: "Wartungsfenster",
    lastAppliedRelease: "Zuletzt angewendetes Release",
    updateSource: "Update-Quelle",
    autoDownload: "Automatischer Download",
    authRequired: "Management-Authentifizierung erforderlich.",
    unknownError: "Unbekannter Fehler.",
    login502: "Management-API nicht erreichbar (502). Bitte lpe-ct.service und nginx pruefen.",
    authenticated: "Authentifiziert.",
    savedSite: "DMZ-Profil gespeichert.",
    savedRelay: "LAN-Relay gespeichert.",
    savedNetwork: "Netzwerkoberflaeche gespeichert.",
    savedPolicies: "Sortierrichtlinien gespeichert.",
    savedUpdates: "Updatepolitik gespeichert.",
    statusDrain: "Drain-Modus",
    statusProduction: "Produktion",
    relayReachable: "LAN-Relay erreichbar",
    relayUnreachable: "LAN-Relay nicht erreichbar",
    heroSummaryTemplate: "{dmzZone} · MX {publishedMx} · primaeres Relay {primaryUpstream}",
  },
  it: {
    pageTitle: "Console di gestione LPE-CT",
    languageLabel: "Lingua",
    brand: "La Poste Electronique",
    loginTitle: "Accesso gestione LPE-CT",
    loginCopy:
      "Il primo accesso usa l'amministratore bootstrap. Cambiare il segreto bootstrap nella configurazione di distribuzione dopo la validazione iniziale.",
    adminEmail: "Email amministratore",
    password: "Password",
    signIn: "Accedi",
    workspace: "Workspace",
    governance: "Governance",
    consoleTitle: "Centro di Smistamento",
    refresh: "Aggiorna",
    heroKicker: "Centro di smistamento DMZ",
    heroLoadingTitle: "Caricamento...",
    heroLoadingSummary: "Lettura dello stato del centro di smistamento.",
    consoleIntro:
      "Interfaccia di gestione per un nodo DMZ distinto dalla LAN, focalizzata su routing, quarantena, politiche di ingresso e aggiornamenti.",
    refreshState: "Aggiorna stato",
    policiesShort: "Politiche",
    relayShort: "Relay",
    pillBoundary: "Confine DMZ",
    pillMagika: "Compatibile Magika",
    pillQueue: "Disciplina code",
    inbound: "In entrata",
    deferred: "Differiti",
    quarantine: "Quarantena",
    attemptsPerHour: "Tentativi / h",
    configTitle: "Configurazione del centro di smistamento",
    configSummary: "Selezionare una sezione per aprire il pannello di gestione.",
    quickActions: "Azioni rapide",
    quickActionsCopy: "Operazioni frequenti per il nodo DMZ.",
    reviewIdentity: "Rivedi identita",
    reviewNetwork: "Rivedi rete",
    planUpdate: "Pianifica aggiornamento",
    quarantineFocus: "Focus quarantena",
    quarantineFocusCopy: "Elementi che devono restare a un clic per la revisione perimetrale.",
    quarantineItemOneTitle: "invoice-0427.zip",
    quarantineItemOneCopy: "Rilevato come payload esotico da Magika. Bloccato in LPE-CT.",
    quarantineItemTwoTitle: "board-minutes.p7m",
    quarantineItemTwoCopy: "Payload cifrato segnato come non ispezionabile. In attesa di revisione della politica.",
    pendingReview: "Revisione in attesa",
    encrypted: "Cifrato",
    drawerConfig: "Configurazione",
    drawerSummaryDefault: "Modificare la sezione selezionata.",
    close: "Chiudi",
    saveSite: "Salva profilo",
    saveRelay: "Salva relay",
    saveNetwork: "Salva rete",
    savePolicies: "Salva politiche",
    saveUpdates: "Salva aggiornamenti",
    auditTitle: "Registro recente",
    auditCopy: "Traccia breve delle modifiche di configurazione del centro di smistamento.",
    drawerSiteTitle: "Identita del nodo",
    drawerSiteSummary: "Posizionamento ed esposizione del server di smistamento nella DMZ.",
    drawerRelayTitle: "Relay verso la LAN",
    drawerRelaySummary: "Flussi strettamente consentiti tra la DMZ e il core LPE.",
    drawerNetworkTitle: "Superficie di rete",
    drawerNetworkSummary: "Restrizione di CIDR di gestione, uscite e listener pubblici.",
    drawerPoliciesTitle: "Politiche di smistamento",
    drawerPoliciesSummary: "Controllo di drain mode, quarantena e requisiti di verifica.",
    drawerUpdatesTitle: "Aggiornamenti",
    drawerUpdatesSummary: "Politica di aggiornamento Debian Git-first per il nodo DMZ.",
    navSiteTitle: "Identita del nodo",
    navRelayTitle: "Relay verso la LAN",
    navNetworkTitle: "Superficie di rete",
    navPoliciesTitle: "Politiche di smistamento",
    navUpdatesTitle: "Aggiornamenti",
    navSiteCopy: "Posizionamento ed esposizione del server di smistamento nella DMZ.",
    navRelayCopy: "Flussi strettamente consentiti tra la DMZ e il core LPE.",
    navNetworkCopy: "Restrizione di CIDR di gestione, uscite e listener pubblici.",
    navPoliciesCopy: "Controllo di drain mode, quarantena e requisiti di verifica.",
    navUpdatesCopy: "Politica di aggiornamento Debian Git-first per il nodo DMZ.",
    managementSiteTitle: "Identita del nodo",
    managementSiteCopy: "Nome, ruolo, regione, FQDN e interfacce pubblicate.",
    managementSitePill: "Profilo DMZ",
    managementRelayTitle: "Relay verso la LAN",
    managementRelayCopy: "Upstream, mTLS, hold queue e dipendenze LAN.",
    managementRelayPill: "Trasporto",
    managementNetworkTitle: "Superficie di rete",
    managementNetworkCopy: "CIDR consentiti, smart host, listener e proxy protocol.",
    managementNetworkPill: "DMZ",
    managementPoliciesTitle: "Politiche di smistamento",
    managementPoliciesCopy: "SPF, DKIM, DMARC, greylisting, quarantena e dimensione messaggio.",
    managementPoliciesPill: "Sicurezza",
    managementUpdatesTitle: "Aggiornamenti",
    managementUpdatesCopy: "Canale, finestra di manutenzione, sorgente Git e politica di download.",
    managementUpdatesPill: "Operazioni",
    open: "Apri",
    nodeName: "Nome nodo",
    role: "Ruolo",
    region: "Regione",
    dmzZone: "Zona DMZ",
    publishedMx: "MX pubblicato",
    managementFqdn: "FQDN gestione",
    publicSmtpBind: "Bind SMTP pubblico",
    managementBind: "Bind gestione",
    primaryUpstream: "Upstream primario",
    secondaryUpstream: "Upstream secondario",
    syncInterval: "Intervallo sync (s)",
    lanDependencyNote: "Nota dipendenza LAN",
    mutualTlsRequired: "mTLS richiesto",
    fallbackToHoldQueue: "Fallback su hold queue",
    allowedManagementCidrs: "CIDR gestione consentiti",
    allowedUpstreamCidrs: "CIDR upstream consentiti",
    outboundSmartHosts: "Smart host in uscita",
    maxConcurrentSessions: "Sessioni concorrenti max",
    publicListenerEnabled: "Listener pubblico abilitato",
    submissionListenerEnabled: "Listener submission abilitato",
    proxyProtocolEnabled: "Proxy protocol abilitato",
    maxMessageSizeMb: "Dimensione max messaggio (MB)",
    drainMode: "Drain mode",
    quarantineEnabled: "Quarantena abilitata",
    greylistingEnabled: "Greylisting abilitato",
    requireSpf: "Richiedi SPF",
    requireDkimAlignment: "Richiedi allineamento DKIM",
    requireDmarcEnforcement: "Richiedi applicazione DMARC",
    attachmentTextScanEnabled: "Scansione testo allegati abilitata",
    channel: "Canale",
    maintenanceWindow: "Finestra di manutenzione",
    lastAppliedRelease: "Ultima release applicata",
    updateSource: "Sorgente aggiornamento",
    autoDownload: "Download automatico",
    authRequired: "Autenticazione management richiesta.",
    unknownError: "Errore sconosciuto.",
    login502: "API di management irraggiungibile (502). Controllare lpe-ct.service e nginx.",
    authenticated: "Autenticato.",
    savedSite: "Profilo DMZ salvato.",
    savedRelay: "Relay LAN salvato.",
    savedNetwork: "Superficie di rete salvata.",
    savedPolicies: "Politiche di smistamento salvate.",
    savedUpdates: "Politica di aggiornamento salvata.",
    statusDrain: "Drain mode",
    statusProduction: "Produzione",
    relayReachable: "Relay LAN raggiungibile",
    relayUnreachable: "Relay LAN non raggiungibile",
    heroSummaryTemplate: "{dmzZone} · MX {publishedMx} · relay primario {primaryUpstream}",
  },
  es: {
    pageTitle: "Consola de administracion LPE-CT",
    languageLabel: "Idioma",
    brand: "La Poste Electronique",
    loginTitle: "Acceso de administracion LPE-CT",
    loginCopy:
      "El primer acceso usa el administrador bootstrap. Cambie el secreto bootstrap en la configuracion de despliegue despues de la validacion inicial.",
    adminEmail: "Correo del administrador",
    password: "Contrasena",
    signIn: "Iniciar sesion",
    workspace: "Espacio",
    governance: "Gobernanza",
    consoleTitle: "Centro de Clasificacion",
    refresh: "Actualizar",
    heroKicker: "Centro de clasificacion DMZ",
    heroLoadingTitle: "Cargando...",
    heroLoadingSummary: "Leyendo el estado del centro de clasificacion.",
    consoleIntro:
      "Interfaz de administracion para un nodo DMZ distinto de la LAN, centrada en enrutamiento, cuarentena, politicas de entrada y actualizaciones.",
    refreshState: "Actualizar estado",
    policiesShort: "Politicas",
    relayShort: "Relay",
    pillBoundary: "Frontera DMZ",
    pillMagika: "Compatible con Magika",
    pillQueue: "Disciplina de cola",
    inbound: "Entrante",
    deferred: "Diferido",
    quarantine: "Cuarentena",
    attemptsPerHour: "Intentos / h",
    configTitle: "Configuracion del centro de clasificacion",
    configSummary: "Seleccione una seccion para abrir su panel de gestion.",
    quickActions: "Acciones rapidas",
    quickActionsCopy: "Operaciones frecuentes para el nodo DMZ.",
    reviewIdentity: "Revisar identidad",
    reviewNetwork: "Revisar red",
    planUpdate: "Planificar actualizacion",
    quarantineFocus: "Foco de cuarentena",
    quarantineFocusCopy: "Elementos que deben quedar a un clic para la revision perimetral.",
    quarantineItemOneTitle: "invoice-0427.zip",
    quarantineItemOneCopy: "Detectado como carga exotica por Magika. Retenido en LPE-CT.",
    quarantineItemTwoTitle: "board-minutes.p7m",
    quarantineItemTwoCopy: "Carga cifrada marcada como no inspeccionable. En espera de revision de politica.",
    pendingReview: "Revision pendiente",
    encrypted: "Cifrado",
    drawerConfig: "Configuracion",
    drawerSummaryDefault: "Modificar la seccion seleccionada.",
    close: "Cerrar",
    saveSite: "Guardar perfil",
    saveRelay: "Guardar relay",
    saveNetwork: "Guardar red",
    savePolicies: "Guardar politicas",
    saveUpdates: "Guardar actualizaciones",
    auditTitle: "Registro reciente",
    auditCopy: "Traza corta de los cambios de configuracion del centro de clasificacion.",
    drawerSiteTitle: "Identidad del nodo",
    drawerSiteSummary: "Posicionamiento y exposicion del servidor de clasificacion en la DMZ.",
    drawerRelayTitle: "Relay hacia la LAN",
    drawerRelaySummary: "Flujos estrictamente permitidos entre la DMZ y el nucleo LPE.",
    drawerNetworkTitle: "Superficie de red",
    drawerNetworkSummary: "Restriccion de CIDR de gestion, salidas y listeners publicos.",
    drawerPoliciesTitle: "Politicas de clasificacion",
    drawerPoliciesSummary: "Control de drain mode, cuarentena y requisitos de verificacion.",
    drawerUpdatesTitle: "Actualizaciones",
    drawerUpdatesSummary: "Politica de actualizacion Debian Git-first para el nodo DMZ.",
    navSiteTitle: "Identidad del nodo",
    navRelayTitle: "Relay hacia la LAN",
    navNetworkTitle: "Superficie de red",
    navPoliciesTitle: "Politicas de clasificacion",
    navUpdatesTitle: "Actualizaciones",
    navSiteCopy: "Posicionamiento y exposicion del servidor de clasificacion en la DMZ.",
    navRelayCopy: "Flujos estrictamente permitidos entre la DMZ y el nucleo LPE.",
    navNetworkCopy: "Restriccion de CIDR de gestion, salidas y listeners publicos.",
    navPoliciesCopy: "Control de drain mode, cuarentena y requisitos de verificacion.",
    navUpdatesCopy: "Politica de actualizacion Debian Git-first para el nodo DMZ.",
    managementSiteTitle: "Identidad del nodo",
    managementSiteCopy: "Nombre, rol, region, FQDN e interfaces publicadas.",
    managementSitePill: "Perfil DMZ",
    managementRelayTitle: "Relay hacia la LAN",
    managementRelayCopy: "Upstreams, mTLS, hold queue y dependencias LAN.",
    managementRelayPill: "Transporte",
    managementNetworkTitle: "Superficie de red",
    managementNetworkCopy: "CIDR permitidos, smart hosts, listeners y proxy protocol.",
    managementNetworkPill: "DMZ",
    managementPoliciesTitle: "Politicas de clasificacion",
    managementPoliciesCopy: "SPF, DKIM, DMARC, greylisting, cuarentena y tamano de mensaje.",
    managementPoliciesPill: "Seguridad",
    managementUpdatesTitle: "Actualizaciones",
    managementUpdatesCopy: "Canal, ventana de mantenimiento, fuente Git y politica de descarga.",
    managementUpdatesPill: "Operaciones",
    open: "Abrir",
    nodeName: "Nombre del nodo",
    role: "Rol",
    region: "Region",
    dmzZone: "Zona DMZ",
    publishedMx: "MX publicado",
    managementFqdn: "FQDN de gestion",
    publicSmtpBind: "Bind SMTP publico",
    managementBind: "Bind de gestion",
    primaryUpstream: "Upstream primario",
    secondaryUpstream: "Upstream secundario",
    syncInterval: "Intervalo de sync (s)",
    lanDependencyNote: "Nota de dependencia LAN",
    mutualTlsRequired: "mTLS requerido",
    fallbackToHoldQueue: "Fallback a hold queue",
    allowedManagementCidrs: "CIDR de gestion permitidos",
    allowedUpstreamCidrs: "CIDR upstream permitidos",
    outboundSmartHosts: "Smart hosts salientes",
    maxConcurrentSessions: "Sesiones concurrentes maximas",
    publicListenerEnabled: "Listener publico habilitado",
    submissionListenerEnabled: "Listener de submission habilitado",
    proxyProtocolEnabled: "Proxy protocol habilitado",
    maxMessageSizeMb: "Tamano maximo de mensaje (MB)",
    drainMode: "Drain mode",
    quarantineEnabled: "Cuarentena habilitada",
    greylistingEnabled: "Greylisting habilitado",
    requireSpf: "Exigir SPF",
    requireDkimAlignment: "Exigir alineacion DKIM",
    requireDmarcEnforcement: "Exigir aplicacion DMARC",
    attachmentTextScanEnabled: "Escaneo de texto de adjuntos habilitado",
    channel: "Canal",
    maintenanceWindow: "Ventana de mantenimiento",
    lastAppliedRelease: "Ultima version aplicada",
    updateSource: "Fuente de actualizacion",
    autoDownload: "Descarga automatica",
    authRequired: "Autenticacion de administracion requerida.",
    unknownError: "Error desconocido.",
    login502: "API de administracion inaccesible (502). Revise lpe-ct.service y nginx.",
    authenticated: "Autenticado.",
    savedSite: "Perfil DMZ guardado.",
    savedRelay: "Relay LAN guardado.",
    savedNetwork: "Superficie de red guardada.",
    savedPolicies: "Politicas de clasificacion guardadas.",
    savedUpdates: "Politica de actualizacion guardada.",
    statusDrain: "Drain mode",
    statusProduction: "Produccion",
    relayReachable: "Relay LAN accesible",
    relayUnreachable: "Relay LAN inaccesible",
    heroSummaryTemplate: "{dmzZone} · MX {publishedMx} · relay primario {primaryUpstream}",
  },
};

let currentLocale = getInitialLocale();
let lastDashboard = null;

const loginEmailField = document.querySelector("#login-form input[name='email']");
if (loginEmailField) {
  loginEmailField.value = window.localStorage.getItem(LAST_ADMIN_EMAIL_KEY) ?? "";
}

function getCopy() {
  return messages[currentLocale] ?? messages.en;
}

function getInitialLocale() {
  const stored = window.localStorage.getItem(LOCALE_KEY);
  return supportedLocales.includes(stored) ? stored : "en";
}

function setLocale(locale) {
  currentLocale = supportedLocales.includes(locale) ? locale : "en";
  window.localStorage.setItem(LOCALE_KEY, currentLocale);
  document.documentElement.lang = currentLocale;
  document.title = getCopy().pageTitle;
  syncLocalePickers();
  applyTranslations();
  if (lastDashboard) {
    render(lastDashboard);
  }
}

function syncLocalePickers() {
  localePickers.forEach((picker) => {
    picker.value = currentLocale;
  });
}

function translate(template, values = {}) {
  return template.replace(/\{(\w+)\}/g, (_, key) => String(values[key] ?? ""));
}

function applyTranslations() {
  const copy = getCopy();

  document.querySelectorAll("[data-i18n]").forEach((element) => {
    const key = element.dataset.i18n;
    if (key && copy[key]) {
      element.textContent = copy[key];
    }
  });

  document.querySelectorAll("[data-i18n-placeholder]").forEach((element) => {
    const key = element.dataset.i18nPlaceholder;
    if (key && copy[key]) {
      element.setAttribute("placeholder", copy[key]);
    }
  });

  document.querySelectorAll("[data-i18n-aria-label]").forEach((element) => {
    const key = element.dataset.i18nAriaLabel;
    if (key && copy[key]) {
      element.setAttribute("aria-label", copy[key]);
    }
  });

  panelTriggers.forEach((trigger) => {
    const titleKey = trigger.dataset.panelTitleKey;
    const summaryKey = trigger.dataset.panelSummaryKey;
    if (titleKey && copy[titleKey]) {
      trigger.dataset.title = copy[titleKey];
    }
    if (summaryKey && copy[summaryKey]) {
      trigger.dataset.summary = copy[summaryKey];
    }
  });

  if (configDrawer && !configDrawer.classList.contains("hidden")) {
    const activeTrigger = panelTriggers.find((trigger) => trigger.classList.contains("is-active"));
    if (activeTrigger) {
      drawerTitle.textContent = activeTrigger.dataset.title ?? copy.drawerConfig;
      drawerSummary.textContent = activeTrigger.dataset.summary ?? copy.drawerSummaryDefault;
    }
  }
}

function authHeaders() {
  const token = window.localStorage.getItem(AUTH_TOKEN_KEY);
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function fetchDashboard() {
  const response = await fetch("/api/dashboard", { headers: authHeaders() });
  if (!response.ok) {
    throw new Error(`dashboard request failed: ${response.status}`);
  }
  return response.json();
}

async function submitForm(path, payload, successMessage) {
  const response = await fetch(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(`request failed: ${response.status}`);
  }

  const dashboard = await response.json();
  render(dashboard);
  closeDrawer();
  showFeedback(successMessage, false);
}

function openDrawer(panelId, title, summary) {
  drawerPanels.forEach((panel) => {
    panel.classList.toggle("hidden", panel.id !== panelId);
  });
  panelTriggers.forEach((trigger) => {
    trigger.classList.toggle("is-active", trigger.dataset.openPanel === panelId);
  });
  drawerTitle.textContent = title;
  drawerSummary.textContent = summary;
  configDrawer.classList.remove("hidden");
}

function closeDrawer() {
  panelTriggers.forEach((trigger) => trigger.classList.remove("is-active"));
  configDrawer.classList.add("hidden");
}

function showFeedback(message, isError) {
  feedback.textContent = message;
  feedback.className = isError ? "feedback error" : "feedback";
}

function showLoginFeedback(message, isError) {
  loginFeedback.textContent = message;
  loginFeedback.className = isError ? "feedback error" : "feedback";
}

function csvLines(text) {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function assignValues(form, values) {
  Object.entries(values).forEach(([key, value]) => {
    const field = form.elements.namedItem(key);
    if (!field) {
      return;
    }

    if (field.type === "checkbox") {
      field.checked = Boolean(value);
      return;
    }

    field.value = Array.isArray(value) ? value.join("\n") : String(value);
  });
}

function renderAudit(audit) {
  const container = document.getElementById("audit-log");
  container.innerHTML = "";

  audit.forEach((entry) => {
    const row = document.createElement("article");
    row.className = "audit-entry";
    row.innerHTML = `<strong>${entry.action}</strong><span>${entry.actor}</span><span>${entry.timestamp}</span><p>${entry.details}</p>`;
    container.appendChild(row);
  });
}

function render(dashboard) {
  lastDashboard = dashboard;
  const copy = getCopy();
  document.getElementById("node-name").textContent = dashboard.site.node_name;
  document.getElementById("hero-summary").textContent = translate(copy.heroSummaryTemplate, {
    dmzZone: dashboard.site.dmz_zone,
    publishedMx: dashboard.site.published_mx,
    primaryUpstream: dashboard.relay.primary_upstream,
  });
  document.getElementById("status-badge").textContent = dashboard.policies.drain_mode ? copy.statusDrain : copy.statusProduction;
  document.getElementById("status-badge").className = dashboard.policies.drain_mode ? "badge warn" : "badge ok";
  document.getElementById("upstream-badge").textContent = dashboard.queues.upstream_reachable ? copy.relayReachable : copy.relayUnreachable;
  document.getElementById("upstream-badge").className = dashboard.queues.upstream_reachable ? "badge ok" : "badge danger";

  document.getElementById("metric-inbound").textContent = dashboard.queues.inbound_messages;
  document.getElementById("metric-deferred").textContent = dashboard.queues.deferred_messages;
  document.getElementById("metric-quarantine").textContent = dashboard.queues.quarantined_messages;
  document.getElementById("metric-attempts").textContent = dashboard.queues.delivery_attempts_last_hour;

  assignValues(document.getElementById("site-form"), dashboard.site);
  assignValues(document.getElementById("relay-form"), dashboard.relay);
  assignValues(document.getElementById("network-form"), dashboard.network);
  assignValues(document.getElementById("policies-form"), dashboard.policies);
  assignValues(document.getElementById("updates-form"), dashboard.updates);
  renderAudit(dashboard.audit);
}

function formPayloads() {
  return {
    site: () => {
      const form = document.getElementById("site-form");
      return Object.fromEntries(new FormData(form).entries());
    },
    relay: () => {
      const form = document.getElementById("relay-form");
      return {
        primary_upstream: form.elements.namedItem("primary_upstream").value,
        secondary_upstream: form.elements.namedItem("secondary_upstream").value,
        sync_interval_seconds: Number(form.elements.namedItem("sync_interval_seconds").value),
        lan_dependency_note: form.elements.namedItem("lan_dependency_note").value,
        mutual_tls_required: form.elements.namedItem("mutual_tls_required").checked,
        fallback_to_hold_queue: form.elements.namedItem("fallback_to_hold_queue").checked,
      };
    },
    network: () => {
      const form = document.getElementById("network-form");
      return {
        allowed_management_cidrs: csvLines(form.elements.namedItem("allowed_management_cidrs").value),
        allowed_upstream_cidrs: csvLines(form.elements.namedItem("allowed_upstream_cidrs").value),
        outbound_smart_hosts: csvLines(form.elements.namedItem("outbound_smart_hosts").value),
        public_listener_enabled: form.elements.namedItem("public_listener_enabled").checked,
        submission_listener_enabled: form.elements.namedItem("submission_listener_enabled").checked,
        proxy_protocol_enabled: form.elements.namedItem("proxy_protocol_enabled").checked,
        max_concurrent_sessions: Number(form.elements.namedItem("max_concurrent_sessions").value),
      };
    },
    policies: () => {
      const form = document.getElementById("policies-form");
      return {
        drain_mode: form.elements.namedItem("drain_mode").checked,
        quarantine_enabled: form.elements.namedItem("quarantine_enabled").checked,
        greylisting_enabled: form.elements.namedItem("greylisting_enabled").checked,
        require_spf: form.elements.namedItem("require_spf").checked,
        require_dkim_alignment: form.elements.namedItem("require_dkim_alignment").checked,
        require_dmarc_enforcement: form.elements.namedItem("require_dmarc_enforcement").checked,
        attachment_text_scan_enabled: form.elements.namedItem("attachment_text_scan_enabled").checked,
        max_message_size_mb: Number(form.elements.namedItem("max_message_size_mb").value),
      };
    },
    updates: () => {
      const form = document.getElementById("updates-form");
      return {
        channel: form.elements.namedItem("channel").value,
        auto_download: form.elements.namedItem("auto_download").checked,
        maintenance_window: form.elements.namedItem("maintenance_window").value,
        last_applied_release: form.elements.namedItem("last_applied_release").value,
        update_source: form.elements.namedItem("update_source").value,
      };
    },
  };
}

async function load() {
  try {
    const dashboard = await fetchDashboard();
    render(dashboard);
    loginShell.classList.add("hidden");
    consoleShell.classList.remove("hidden");
    feedback.className = "feedback hidden";
  } catch (error) {
    if (error instanceof Error && error.message.includes("401")) {
      window.localStorage.removeItem(AUTH_TOKEN_KEY);
      consoleShell.classList.add("hidden");
      loginShell.classList.remove("hidden");
      showLoginFeedback(getCopy().authRequired, true);
      return;
    }
    showFeedback(error instanceof Error ? error.message : getCopy().unknownError, true);
  }
}

async function loginAdmin() {
  const form = document.getElementById("login-form");
  const payload = Object.fromEntries(new FormData(form).entries());
  const response = await fetch("/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    if (response.status === 502) {
      throw new Error(getCopy().login502);
    }
    throw new Error(`login request failed: ${response.status}`);
  }
  const body = await response.json();
  window.localStorage.setItem(AUTH_TOKEN_KEY, body.token);
  if (typeof payload.email === "string" && payload.email.trim()) {
    window.localStorage.setItem(LAST_ADMIN_EMAIL_KEY, payload.email.trim());
  }
  showLoginFeedback(getCopy().authenticated, false);
  await load();
}

localePickers.forEach((picker) => {
  supportedLocales.forEach((locale) => {
    const option = document.createElement("option");
    option.value = locale;
    option.textContent = localeLabels[locale];
    picker.appendChild(option);
  });
  picker.addEventListener("change", (event) => {
    setLocale(event.target.value);
  });
});

document.getElementById("refresh").addEventListener("click", () => {
  void load();
});

const refreshToolbar = document.getElementById("refresh-toolbar");
if (refreshToolbar) {
  refreshToolbar.addEventListener("click", () => {
    void load();
  });
}

panelTriggers.forEach((button) => {
  button.addEventListener("click", () => {
    openDrawer(button.dataset.openPanel, button.dataset.title, button.dataset.summary);
  });
});

document.getElementById("drawer-close").addEventListener("click", closeDrawer);

configDrawer.addEventListener("click", (event) => {
  if (event.target === configDrawer) {
    closeDrawer();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !configDrawer.classList.contains("hidden")) {
    closeDrawer();
  }
});

const payloads = formPayloads();

document.getElementById("site-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/site", payloads.site(), getCopy().savedSite);
});

document.getElementById("relay-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/relay", payloads.relay(), getCopy().savedRelay);
});

document.getElementById("network-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/network", payloads.network(), getCopy().savedNetwork);
});

document.getElementById("policies-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/policies", payloads.policies(), getCopy().savedPolicies);
});

document.getElementById("updates-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void submitForm("/api/updates", payloads.updates(), getCopy().savedUpdates);
});

document.getElementById("login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void loginAdmin().catch((error) => {
    showLoginFeedback(error instanceof Error ? error.message : getCopy().unknownError, true);
  });
});

setLocale(currentLocale);
void load();
