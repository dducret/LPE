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

### Roles

- administrateur serveur
- administrateur domaine
- operateur transport
- role compliance / audit
- support / helpdesk
- utilisateur final

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

### Roles

- server administrator
- domain administrator
- transport operator
- compliance / audit role
- support / helpdesk
- end user

### Administration pattern

The default administration pattern is:

- full-width list
- primary `New` or `Create` action
- right-side drawer for creation, details, and contextual actions

Those drawers must be deep-linkable.
