# Politique de licences

## Regle de base

- tout code source produit dans `LPE` doit etre publie sous `Apache-2.0`
- les dependances `MIT` sont autorisees uniquement lorsqu'aucune alternative `Apache-2.0` raisonnable n'existe
- les dependances `GPL`, `LGPL`, `AGPL`, `SSPL` et licences non standard sont interdites

## Procedure d'ajout d'une dependance

1. verifier la licence declaree
2. rechercher une alternative `Apache-2.0`
3. documenter la justification si une dependance `MIT` est retenue
4. ajouter un controle automatique en CI des licences

## Liste initiale des exceptions MIT a justifier

- `tokio`
- `axum`
- `tracing`

Ces dependances sont courantes dans l'ecosysteme Rust et retenues provisoirement pour accelerer le demarrage. Une revue documentaire devra etre maintenue pour chaque exception.

