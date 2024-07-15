package uk.ac.ebi.complex.service;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collection;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class ComplexFinderResult<T> {
    private Collection<String> proteins;
    private Collection<ExactMatch<T>> exactMatches;
    private Collection<PartialMatch<T>> partialMatches;

    public enum MatchType {
        EXACT_MATCH,
        EXACT_MATCH_AT_PROTEIN_LEVEL,
        PARTIAL_MATCH_SUBSET_OF_COMPLEX,
        PARTIAL_MATCH_PROTEINS_MISSING_IN_COMPLEX,
        PARTIAL_MATCH_OTHER
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @EqualsAndHashCode
    public static class ExactMatch<T> {
        private String complexAc;
        private MatchType matchType;
        private T complex;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @EqualsAndHashCode
    public static class PartialMatch<T> {
        private String complexAc;
        private MatchType matchType;
        private Collection<String> matchingProteins;
        private Collection<String> extraProteinsInComplex;
        private Collection<String> proteinMissingInComplex;
        private T complex;
    }
}
