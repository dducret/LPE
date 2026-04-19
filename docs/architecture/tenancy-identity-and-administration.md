# Tenancy, Identity, and Administration | Tenancy, identite et administration

## Francais

### Objectif

Ce document decrit le modele multi-tenant, l'identite et les roles d'administration.

### Multi-tenant

`LPE` est multi-tenant.

Chaque tenant gere son domaine et les mailbox du domaine.

Seul `LPE-CT` en `DMZ` est mutualise entre domaines.

### Identite moderne

Pour une plateforme multi-tenant moderne, le support natif de `OAuth2` et `OIDC` est requis.

Pour le plan d'administration, `LPE` supporte maintenant un premier MVP de login federé fonde sur le code flow `OIDC` confidentiel, tout en conservant le login local par mot de passe pour le bootstrap, la recuperation et le fallback.

Le scope initial reste volontairement limite:

- le login federé concerne actuellement le back office d'administration
- le login des comptes mailbox reste base sur mot de passe en v1
- aucun mode passwordless-only n'est requis en v1
- la base reserve deja des facteurs d'authentification pour une prise en charge future de `TOTP`

### Federation admin MVP

Cette premiere brique suit les regles suivantes:

- le login local par mot de passe reste disponible tant qu'un administrateur ne le desactive pas explicitement
- le login `OIDC` doit correspondre a une identite administrateur `LPE` existante
- aucun administrateur n'est cree automatiquement depuis l'`IdP` en v1
- un auto-link optionnel par email peut rattacher une identite federée a un administrateur deja existant avec la meme adresse
- la configuration du fournisseur est globale au back office coeur `LPE`
- le callback doit etre aligne avec l'origine publique reelle exposee par le reverse proxy

Dans ce MVP, l'`IdP` authentifie l'identite, mais l'autorisation reste interne a `LPE`.

### Roles

- administrateur serveur
- administrateur domaine
- operateur transport
- role compliance / audit
- support / helpdesk
- utilisateur final

Les roles sont maintenant couples a des permissions structurees et normalisees.

Les roles integres fournissent des permissions par defaut, tandis que des permissions explicites peuvent etre ajoutees pour des delegations d'exception.

Le login mot de passe et le login `OIDC` resolvent tous deux vers le meme modele interne de role et de permissions.

### Preparation MFA

Le modele d'authentification enregistre maintenant la methode d'authentification utilisee par chaque session admin et reserve une table dediee aux facteurs d'authentification admin.

Il s'agit uniquement d'une preparation. `TOTP`, recovery codes, step-up policies et UX d'enrolement restent du travail futur.

### Pattern d'administration

Le pattern par defaut est:

- liste pleine largeur
- action principale `New` ou `Create`
- drawer lateral droit pour creation, details et actions

Ces drawers doivent etre deep-linkable.

## English

### Goal

This document describes the multi-tenant model, identity, and administration roles.

### Multi-tenancy

`LPE` is multi-tenant.

Each tenant manages its own domain and the mailboxes of that domain.

Only `LPE-CT` in the `DMZ` is shared across domains.

### Modern identity

For a modern multi-tenant platform, native `OAuth2` and `OIDC` support is required.

For the administration plane, `LPE` now supports a first federated-login MVP based on confidential `OIDC` code flow, while keeping local password login available for bootstrap, recovery, and fallback.

The initial scope stays intentionally limited:

- federated login currently applies to the administration back office
- mailbox-account login remains password-based in v1
- no passwordless-only mode is required in v1
- the database already reserves authentication-factor records for later `TOTP` support

### Federated admin MVP

This first building block follows these rules:

- local password login stays available unless an administrator explicitly disables it
- `OIDC` login must resolve to an existing `LPE` administrator identity
- no administrator is automatically created from the `IdP` in v1
- optional email auto-link may bind a federated identity to an already existing administrator with the same address
- provider configuration is global to the core `LPE` administration plane
- the callback must match the real public origin exposed by the reverse proxy

In this MVP, the `IdP` authenticates the identity, but authorization stays inside `LPE`.

### Roles

- server administrator
- domain administrator
- transport operator
- compliance / audit role
- support / helpdesk
- end user

Roles are now paired with structured normalized permissions.

Built-in roles provide default permissions, while explicit permissions may be added for exceptional delegations.

Password login and `OIDC` login both resolve to the same internal role and permission model.

### MFA preparation

The authentication model now records the authentication method used by each admin session and reserves a dedicated table for administrator authentication factors.

This is only a preparation step. `TOTP`, recovery codes, step-up policies, and enrollment UX remain future work.

### Administration pattern

The default administration pattern is:

- full-width list
- primary `New` or `Create` action
- right-side drawer for creation, details, and contextual actions

Those drawers must be deep-linkable.
