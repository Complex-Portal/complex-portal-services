# Complex Finder

## Introduction

The Uniplex Import is a Java library used to find complexes based on the proteins that formed them.

## ComplexFinder

The [ComplexFinder class](src/main/java/uk/ac/ebi/complex/service/finder/ComplexFinder.java) defines a public method
`findComplexWithMatchingProteins` to find complexes that takes a list of protein ids and options to configure how
to find complexes.

These options are:
- checkPredictedComplexes: if true, curated and predicted complexes are checked; if false, only curated complexes are checked.
- checkAnyStatusForExactMatches: if true, any complex is checked; if false, only complexes released or ready for release are checked.
- checkPartialMatches: if true, partial matches are also returned; if false, only exact matches are returned.

## ComplexOrthologFinder

The [ComplexOrthologFinder class](src/main/java/uk/ac/ebi/complex/service/finder/ComplexOrthologFinder.java) defines a public method
`findComplexOrthologs` to find related complexes (orthologs, paralogs, etc.) for the given complex, by checking if the proteins
of the components are orthologs, and the cellular components if specified.

It takes the following arguments:
- complexId: id of the complex to find orthologs of.
- taxId: if null, the method looks for orthologs on any species; if set, it looks for orthologs on the specified species
- config: configuration options for the ortholog finder logic
  - checkCellularComponentsForCurated: if set, the method also checks if curated complexes have matching cellular components
    with the given complex; otherwise only components are checked.
  - - checkCellularComponentsForCurated: if set, the method also checks if predicted complexes have matching cellular components
    with the given complex; otherwise only components are checked.
