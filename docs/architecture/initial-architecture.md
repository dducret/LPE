# Architecture initiale | Initial architecture

## Francais

### Decisions de depart

- stockage primaire : `PostgreSQL`
- axe protocolaire moderne : `JMAP`
- compatibilite initiale : `SMTP`, `IMAP`
- code LPE : `Apache-2.0`
- dependances : `Apache-2.0` d'abord, `MIT` uniquement par exception justifiee
- architecture de donnees preparee pour une IA locale future
- possibilite de deporter le centre de tri sur un serveur `DMZ` distinct sans deplacer le coeur metier

### Vision

`LPE` est un serveur de messagerie et de collaboration moderne. Le coeur metier ne depend pas de `IMAP` ni de `SMTP`. Les protocoles externes sont des adaptateurs autour d'un modele interne stable.

Les clients natifs restent un objectif important. Un utilisateur doit pouvoir connecter sa boite `LPE` depuis un client mobile ou desktop compatible, par exemple l'application Mail de l'iPhone, sans perdre la coherence de sa boite entre protocoles.

Cela implique que tout envoi realise via `SMTP` submission, `IMAP`, `ActiveSync`, `EWS` ou `JMAP` alimente la meme representation canonique du message dans `LPE`, y compris la copie du message envoye dans la vue `Sent`.

### Blocs principaux

1. `lpe-domain`
Types metier partages.

2. `lpe-core`
Regles applicatives et orchestration du domaine.

3. `lpe-storage`
Adaptateur de persistance `PostgreSQL` et stockage blobs plus tard.

4. `lpe-ai`
Contrats et services pour une IA locale future avec provenance.

5. `lpe-jmap`
Point d'entree moderne pour le client web et les futures apps natives.

6. `lpe-admin-api`
Plan de controle pour le back office.

7. `lpe-cli`
Executable de demarrage local du serveur.

8. `nginx` sur Debian
Serveur frontal HTTP pour exposer l'interface d'administration statique et reverse-proxy `/api/` vers `lpe-admin-api`.

9. `LPE-CT` en option pour la DMZ
Centre de tri distinct pour l'entree SMTP exposee, le filtrage perimetrique, la quarantaine et le relais controle vers le coeur `LPE` dans le `LAN`.

### Priorites MVP

- comptes, domaines, alias, quotas
- SMTP entrant et submission
- IMAP
- coherence des messages envoyes entre protocoles clients et vue `Sent`
- webmail HTTPS
- recherche
- administration web
- projections documentaires et artefacts IA locaux
- indexation des pieces jointes `PDF`, `DOCX` et `ODT`
- interfaces web multilingues `en`, `fr`, `de`, `it`, `es`

## English

### Starting decisions

- primary store: `PostgreSQL`
- modern protocol axis: `JMAP`
- initial compatibility: `SMTP`, `IMAP`
- LPE code: `Apache-2.0`
- dependencies: prefer `Apache-2.0`, allow `MIT` only with a documented exception
- data architecture prepared for future local AI
- ability to move the sorting center onto a distinct `DMZ` server without moving the business core

### Vision

`LPE` is a modern mail and collaboration server. The business core must not depend on `IMAP` or `SMTP`. External protocols are adapters around a stable internal model.

Native clients remain an important goal. A user must be able to connect an `LPE` mailbox from a compatible mobile or desktop client, for example the iPhone Mail application, without losing mailbox consistency across protocols.

This implies that every submission path through `SMTP` submission, `IMAP`, `ActiveSync`, `EWS`, or `JMAP` feeds the same canonical message representation in `LPE`, including the authoritative `Sent` mailbox view.

### Main building blocks

1. `lpe-domain`
Shared business types.

2. `lpe-core`
Application rules and domain orchestration.

3. `lpe-storage`
`PostgreSQL` persistence adapter and later blob storage.

4. `lpe-ai`
Contracts and services for future local AI with provenance.

5. `lpe-jmap`
Modern entry point for the web client and future native apps.

6. `lpe-admin-api`
Control plane for the back office.

7. `lpe-cli`
Local server executable.

8. `nginx` on Debian
HTTP front end used to expose the static administration UI and reverse-proxy `/api/` to `lpe-admin-api`.

9. optional `LPE-CT` in the DMZ
Separate sorting center for exposed SMTP ingress, perimeter filtering, quarantine, and controlled relay toward the core `LPE` services on the `LAN`.

### MVP priorities

- accounts, domains, aliases, quotas
- inbound SMTP and submission
- IMAP
- sent-message consistency across client protocols and the `Sent` view
- HTTPS webmail
- search
- web administration
- document projections and local AI artifacts
- attachment indexing for `PDF`, `DOCX`, and `ODT`
- multilingual web interfaces for `en`, `fr`, `de`, `it`, `es`
