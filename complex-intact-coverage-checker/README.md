# Complex IntAct Coverage Checker

## Introduction

The Complex IntAct Coverage Checker is a Java service used to find and log the interactions found in IntAct for
each pair of proteins in each complex.

## How to run

Run the script [checkComplexIntactCoverage.sh](checkComplexIntactCoverage.sh) to process all complexes of a given species
and log the interactions found.

This script won't actually make any modifications in the database, it will just generate reports with the
complexes and interactions found.

### Input arguments

The script takes the following 6 input arguments:
1. Maven profile
2. IntAct editor username
3. Tax id
4. IntAct GraphDB Web Service URL
5. Directory to write report and output files
6. Separator used in the input and output files
7. Boolean to indicate if the input file has a header

## Process

The steps of this application are:
1. Iterate through the complexes with the specified tax id
2. For each complex:
   1. Check complex status: only complexes ready for checking, ready for release and released are checked.
   2. Call IntAct GraphDB-WS endpoint to find the scores for each protein pair of proteins in the complex.
3. Write reports with the complexes and protein pair scores found.

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
- no_interactions.csv: protein pairs from complexes with no interaction found in IntAct.
- with_interactions.csv: protein pairs from complexes with interactions found in IntAct, and their MI score.
