# Indexation des pieces jointes v1 | v1 attachment indexing

## Francais

### Perimetre

La v1 de `LPE` indexe le texte extrait depuis:

- `PDF`
- `DOCX`
- `ODT`

Les autres formats bureautiques restent hors perimetre v1.

### Bibliotheques retenues

- `pdf_oxide` pour `PDF`
- `docx-lite` pour `DOCX`
- extracteur interne `ODT` base sur ZIP et XML

### Raisons

- respect de la contrainte de licences `Apache-2.0` et `MIT`
- extraction locale cote serveur
- indexation dans `PostgreSQL`
- reutilisable plus tard pour recherche et RAG local

### Strategie

1. valider le type de fichier entrant avec Google `Magika` a partir des bytes reels
2. comparer le type detecte avec le MIME declare et l'extension
3. appliquer la politique d'acceptation, de restriction, de quarantaine ou de rejet
4. detecter le format de la piece jointe retenue
5. extraire le texte brut
6. normaliser le texte
7. stocker le texte dans `attachments.extracted_text`
8. alimenter `attachments.extracted_text_tsv`
9. agreger ensuite la recherche message et pieces jointes

La validation `Magika` est obligatoire pour toute piece jointe ou fichier entrant via connexion externe ou via un client avant indexation, import, ou parsing specialise.

### Hors perimetre v1

- OCR des PDF scannes
- `ODS`, `XLSX`, `PPTX`, `ODP`
- extraction semantique avancee

## English

### Scope

`LPE` v1 indexes extracted text from:

- `PDF`
- `DOCX`
- `ODT`

Other office formats remain out of scope for v1.

### Selected libraries

- `pdf_oxide` for `PDF`
- `docx-lite` for `DOCX`
- internal `ODT` extractor based on ZIP and XML

### Reasons

- respects the `Apache-2.0` and `MIT` license constraint
- server-side local extraction
- indexing in `PostgreSQL`
- reusable later for search and local RAG

### Strategy

1. validate the incoming file type with Google `Magika` from the actual bytes
2. compare the detected type with the declared MIME type and extension
3. apply the acceptance, restriction, quarantine, or rejection policy
4. detect the attachment format for the retained file
5. extract raw text
6. normalize the text
7. store text in `attachments.extracted_text`
8. populate `attachments.extracted_text_tsv`
9. aggregate message and attachment search afterward

`Magika` validation is mandatory for every attachment or file entering through an external connection or through a client before indexing, import, or specialized parsing.

### Out of scope for v1

- OCR for scanned PDFs
- `ODS`, `XLSX`, `PPTX`, `ODP`
- advanced semantic extraction
