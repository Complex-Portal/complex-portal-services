# Complex Batch Import

## Introduction

The Complex Batch Import is a Java service with common logic to process files containing complexes from different resources
and import them into the Complex Portal database.

This service does not have any job to actually import complexes into the Complex Portal database, but it's used as the
backbone of other services with a common process.

## Process

The basic steps to process and import complexes are:
1. Validate and clean input file:
   1. filter out large complexes
   2. validate and clean UniProt ids or gene names
   3. check for duplicate complexes and merge
2. Process and import:
   1. For each complex, find exact and partial matches
      1. For each exact match, check if a cross-reference (identity) and confidence or name should be added to the complex
      2. For each partial match, check if a cross-reference (subset or complex-cluster) and confidence or name should be added to the complex
      3. For complexes with no exact matches, a new complex would be created
   2. Import:
      1. When running on dry-mode, log the changes to be made in the database 
      2. When not running on dry-mode, save the new and updated complexes and commit the changes in the database
3. Delete old cross-references
   1. Find cross-references to complexes that are no longer valid
   2. Delete:
      1. When running on dry-mode, log the changes to be made in the database
      2. When not running on dry-mode, delete the cross-references and confidence values and commit the changes in the database

### Maven profile

To run any of the services that extend the Complex Batch Import to import complexes, we need a Maven profile that define
a set of properties to connect to the Complex Portal database.
- db.url: URL of the database to read/write
- db.dialect: Hibernate dialect to use
- db.driver: Hibernate driver to use
- db.user: DB user
- db.password: DB password
- db.hbm2ddl: property to configure Hibernate property 'hibernate.hbm2ddl.auto'

## Output files

The following files are generated based on the matches found between the input complexes and the complexes in the database.
- exact_matches.csv: complexes that match to 1 complex in the database. If missing, cross-references and confidence scores would be added to these complexes.
- multiple_exact_matches.csv: complexes that match to more than 1 complex in the database. These complexes are ignored.
- partial_matches.csv: complexes that partially match to complexes in the database.
- no_matches.csv: complexes that match to no complexes in the database. New complexes would be created for these input complexes.

When running on dry-mode (no changes saved to the database), data is written to the following files.
- complexes_to_create.csv: file with the new complexes that would be created
- complexes_to_update.csv: file with existing complexes that would be updated with new cross-references
- complexes_unchanged.csv: file with existing complexes that match input complexes, already with cross-references, that need no updates
- xrefs_to_delete.csv: file with old cross-references that do not match any of the complexes in the input file and should be deleted

When not running on dry-mode (data is saved to the database), data is written to the following files.
- new_complexes.csv: file with the new complexes created
- updated_complexes.csv: file with the complexes that have been updated with cross-references and confidence scores

Apart from the files listed above, there are different files with errors found during the import:
- ignored.csv: files with complexes that could not be imported, and why.
- process_errors.csv: file with complexes that failed to be processed.
- write_errors.csv: file with complexes that failed to be written to the database.
- delete_process_errors.csv: file with complexes and cross-references that failed to be processed.
- delete_write_errors.csv:  file with complexes and cross-references that failed to be deleted from the database.