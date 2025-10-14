# Complex Ortholog Checker

## Introduction

The Complex Ortholog Checker is a Java service used to find and log the related complexes found for all the complexes
in a species.

## How to run

Run the script [checkComplexOrthologs.sh](checkComplexOrthologs.sh) to process all complexes of a given species
and log the related complexes found in a different specified species.

This script won't actually make any modifications in the database, it will just generate reports with the
complexes found.

### Input arguments

The script takes the following 6 input arguments:
1. Maven profile
2. IntAct editor username
3. Input Tax id
4. Output Tax id
5. Directory to write report and output files
6. Separator used in the input and output files
7. Boolean to indicate if the input file has a header

## Process

The steps of this application are:
1. Iterate through the complexes with the specified tax id
2. For each complex:
   1. If it's a predicted complex, ignore; if it's curated, continue.
   2. Find related complex (orthologs, paralogs, etc.) in the specified species by using the library `complex-finder`.
3. Write reports with the complexes and the orthologs found.

### Maven profile

To run any the service, we need a Maven profile that define a set of properties to connect to the Complex Portal database.
- db.url: URL of the database to read/write
- db.dialect: Hibernate dialect to use
- db.driver: Hibernate driver to use
- db.user: DB user
- db.password: DB password
- db.hbm2ddl: property to configure Hibernate property 'hibernate.hbm2ddl.auto'

## Output files

The following files are generated based on the interactions found.
- no_orthologs.csv: complexes with no related complexes found.
- with_orthologs_same_cellular_components: complexes and the related complexes found that have a matching cellular component.
- with_orthologs_different_cellular_components.csv: complexes and the related complexes found that do not have a matching cellular component.
