# JMAP Contacts and Calendars MVP | MVP JMAP Contacts et Calendars

## Francais

### Objectif

Ce document decrit le perimetre `JMAP Contacts` et `JMAP Calendars` actuellement supporte par `LPE` pour le MVP.

Le crate `crates/lpe-jmap` agit comme un adaptateur `JMAP` au-dessus des modeles canoniques deja exposes par `lpe-storage`, `CardDAV`, `CalDAV` et `ActiveSync`. Il ne cree aucun stockage, droit, ni logique metier paralleles pour les contacts et le calendrier.

### Principes d'architecture

- `JMAP` reste l'axe moderne principal pour les donnees de collaboration
- `contacts` reste la source de verite pour les fiches contact
- `calendar_events` reste la source de verite pour les evenements calendrier
- `CardDAV`, `CalDAV`, `ActiveSync` et `JMAP` convergent vers les memes objets canoniques
- les droits restent scopes au compte authentifie; il n'existe pas de modele de partage ou de delegation propre a `JMAP`
- aucun code `Stalwart` n'est reutilise

### Authentification

- le client `JMAP` reutilise l'authentification compte existante
- le login reste `/api/mail/auth/login`
- le bearer token de session compte existant doit ensuite etre presente a `/api/jmap/session` et `/api/jmap/api`
- sans reverse proxy Debian, les memes routes sont exposees en direct sous `/jmap/session` et `/jmap/api`

### Capacites de session supportees

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:contacts`
- `urn:ietf:params:jmap:calendars`

La session `JMAP` expose le meme `accountId` que les autres couches protocolaires. Les carnets d'adresses et calendriers sont virtuels au niveau `JMAP`, mais restent branches sur les donnees canoniques du compte courant.

### Methodes supportees

Contacts:

- `AddressBook/get`
- `AddressBook/query`
- `AddressBook/changes`
- `ContactCard/get`
- `ContactCard/query`
- `ContactCard/changes`
- `ContactCard/set`

Calendrier:

- `Calendar/get`
- `Calendar/query`
- `Calendar/changes`
- `CalendarEvent/get`
- `CalendarEvent/query`
- `CalendarEvent/changes`
- `CalendarEvent/set`

### Mapping MVP

#### Address book

- un seul carnet virtuel `default` est expose par compte
- `myRights` indique lecture, ecriture et suppression sur les fiches du compte
- `mayCreateTopLevel` reste `false`

#### ContactCard

Le mapping `JMAP` vers `contacts` est le suivant:

- `id` et `uid` -> `contacts.id`
- `name.full` -> `contacts.name`
- `titles.*.name` -> `contacts.role`
- `emails.*.address` -> `contacts.email`
- `phones.*.number` -> `contacts.phone`
- `organizations.*.name` -> `contacts.team`
- `notes.*.note` -> `contacts.notes`
- `addressBookIds.default` -> collection virtuelle unique du compte

#### Calendar

- un seul calendrier virtuel `default` est expose par compte
- `myRights` indique lecture, ecriture et suppression sur les evenements du compte
- `mayCreateTopLevel` reste `false`

#### CalendarEvent

Le mapping `JMAP` vers `calendar_events` est le suivant:

- `id` et `uid` -> `calendar_events.id`
- `title` -> `calendar_events.title`
- `start` (`YYYY-MM-DDTHH:MM:SS`) -> `calendar_events.date` + `calendar_events.time`
- `locations.*.name` -> `calendar_events.location`
- `participants` -> `calendar_events.attendees` sous forme texte normalisee
- `description` -> `calendar_events.notes`
- `calendarIds.default` -> collection virtuelle unique du compte

### Regles MVP importantes

- `ContactCard/set` cree, remplace ou supprime directement les lignes canoniques de `contacts`
- `CalendarEvent/set` cree, remplace ou supprime directement les lignes canoniques de `calendar_events`
- les lectures `JMAP` utilisent les memes objets comptes que `CardDAV`, `CalDAV` et `ActiveSync`
- le perimetre des droits reste celui du compte authentifie; aucun acces cross-account n'est supporte
- `changes` reexpose l'etat courant du compte et ne maintient pas encore d'historique fin de synchronisation

### Limitations assumees du MVP

- un seul `AddressBook` et un seul `Calendar` virtuels par compte
- pas de partage, delegation, abonnement, ni droits fins par collection
- `ContactCard/query` supporte seulement le tri `name` ascendant et le filtre texte simple, avec `inAddressBook` limite au carnet `default`
- `CalendarEvent/query` supporte seulement le tri `start` ascendant et les filtres `inCalendar`, `text`, `after`, `before`
- `ContactCard/set` supporte seulement `kind=individual`
- `CalendarEvent/set` supporte seulement `@type=Event`
- `CalendarEvent/set` accepte seulement `duration=PT0S`
- `timeZone` doit etre `null` ou absent
- les mises a jour sont des remplacements complets de ressource; les patchs de proprietes fines ne sont pas implementes
- les participants calendrier sont encore stockes comme texte canonique et reexposes en objets `Participant` minimaux
- pas de recurrence, alarmes, disponibilite, statut participant, organisateur, pieces jointes calendrier, ni semantique `VCARD` ou `VCALENDAR` etendue
- les objets `AddressBook` et `Calendar` sont virtuels et non modifiables via `set`

### Coherence avec les autres adaptateurs

- `CardDAV` et `CalDAV` restent les couches de compatibilite DAV sur les memes tables canoniques
- `ActiveSync` reste la couche native Outlook/mobile sur les memes tables canoniques
- `JMAP Contacts` et `JMAP Calendars` deviennent l'acces moderne principal au-dessus de ces memes modeles

## English

### Objective

This document describes the `JMAP Contacts` and `JMAP Calendars` scope currently supported by `LPE` for the MVP.

The `crates/lpe-jmap` crate acts as a `JMAP` adapter on top of the canonical models already exposed through `lpe-storage`, `CardDAV`, `CalDAV`, and `ActiveSync`. It does not introduce any parallel storage, rights, or business logic for contacts and calendars.

### Architectural principles

- `JMAP` remains the primary modern axis for collaboration data
- `contacts` remains the source of truth for contact cards
- `calendar_events` remains the source of truth for calendar events
- `CardDAV`, `CalDAV`, `ActiveSync`, and `JMAP` converge on the same canonical objects
- rights remain scoped to the authenticated account; there is no `JMAP`-specific sharing or delegation model
- no `Stalwart` code is reused

### Authentication

- the `JMAP` client reuses the existing mailbox-account authentication
- login remains `/api/mail/auth/login`
- the existing account bearer token must then be sent to `/api/jmap/session` and `/api/jmap/api`
- without the Debian reverse proxy, the same routes are exposed directly as `/jmap/session` and `/jmap/api`

### Supported session capabilities

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:contacts`
- `urn:ietf:params:jmap:calendars`

The `JMAP` session exposes the same `accountId` as the other protocol adapters. Address books and calendars are virtual at the `JMAP` layer, but remain wired to the authenticated account's canonical data.

### Supported methods

Contacts:

- `AddressBook/get`
- `AddressBook/query`
- `AddressBook/changes`
- `ContactCard/get`
- `ContactCard/query`
- `ContactCard/changes`
- `ContactCard/set`

Calendars:

- `Calendar/get`
- `Calendar/query`
- `Calendar/changes`
- `CalendarEvent/get`
- `CalendarEvent/query`
- `CalendarEvent/changes`
- `CalendarEvent/set`

### MVP mapping

#### Address book

- one virtual `default` address book is exposed per account
- `myRights` advertises read, write, and delete access for the authenticated account's cards
- `mayCreateTopLevel` remains `false`

#### ContactCard

The `JMAP` to `contacts` mapping is:

- `id` and `uid` -> `contacts.id`
- `name.full` -> `contacts.name`
- `titles.*.name` -> `contacts.role`
- `emails.*.address` -> `contacts.email`
- `phones.*.number` -> `contacts.phone`
- `organizations.*.name` -> `contacts.team`
- `notes.*.note` -> `contacts.notes`
- `addressBookIds.default` -> the account's single virtual collection

#### Calendar

- one virtual `default` calendar is exposed per account
- `myRights` advertises read, write, and delete access for the authenticated account's events
- `mayCreateTopLevel` remains `false`

#### CalendarEvent

The `JMAP` to `calendar_events` mapping is:

- `id` and `uid` -> `calendar_events.id`
- `title` -> `calendar_events.title`
- `start` (`YYYY-MM-DDTHH:MM:SS`) -> `calendar_events.date` + `calendar_events.time`
- `locations.*.name` -> `calendar_events.location`
- `participants` -> `calendar_events.attendees` as normalized text
- `description` -> `calendar_events.notes`
- `calendarIds.default` -> the account's single virtual collection

### Important MVP rules

- `ContactCard/set` directly creates, replaces, or deletes canonical `contacts` rows
- `CalendarEvent/set` directly creates, replaces, or deletes canonical `calendar_events` rows
- `JMAP` reads use the same account-scoped objects as `CardDAV`, `CalDAV`, and `ActiveSync`
- rights remain bounded by the authenticated account; cross-account access is not supported
- `changes` re-exposes the current account state and does not yet maintain a fine-grained sync history

### Accepted MVP limitations

- one virtual `AddressBook` and one virtual `Calendar` per account
- no sharing, delegation, subscriptions, or fine-grained collection ACLs
- `ContactCard/query` supports only ascending `name` sort and simple text filtering, with `inAddressBook` limited to the `default` book
- `CalendarEvent/query` supports only ascending `start` sort and the `inCalendar`, `text`, `after`, and `before` filters
- `ContactCard/set` supports only `kind=individual`
- `CalendarEvent/set` supports only `@type=Event`
- `CalendarEvent/set` accepts only `duration=PT0S`
- `timeZone` must be `null` or absent
- updates are full-resource replacements; fine-grained property patches are not implemented
- calendar participants are still stored in the canonical model as text and re-exposed as minimal `Participant` objects
- no recurrence, alarms, free/busy, participant status, organizer model, calendar attachments, or extended `VCARD` or `VCALENDAR` semantics
- `AddressBook` and `Calendar` objects are virtual and cannot be modified through `set`

### Consistency with other adapters

- `CardDAV` and `CalDAV` remain DAV compatibility layers over the same canonical tables
- `ActiveSync` remains the native Outlook/mobile layer over the same canonical tables
- `JMAP Contacts` and `JMAP Calendars` become the primary modern access path over those same models
