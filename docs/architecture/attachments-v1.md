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

1. detecter le format de la piece jointe
2. extraire le texte brut
3. normaliser le texte
4. stocker le texte dans `attachments.extracted_text`
5. alimenter `attachments.extracted_text_tsv`
6. agreger ensuite la recherche message et pieces jointes

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

1. detect the attachment format
2. extract raw text
3. normalize the text
4. store text in `attachments.extracted_text`
5. populate `attachments.extracted_text_tsv`
6. aggregate message and attachment search afterward

### Out of scope for v1

- OCR for scanned PDFs
- `ODS`, `XLSX`, `PPTX`, `ODP`
- advanced semantic extraction
