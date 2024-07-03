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
    private Collection<ComplexMatch<T>> exactMatches;
    private Collection<ComplexMatch<T>> partialMatches;

    public enum MatchType {
        EXACT_MATCH,
        EXACT_MATCH_AT_PROTEIN_LEVEL,
        PARTIAL_MATCH_MISSING_COMPONENTS,
        PARTIAL_MATCH_EXTRA_COMPONENTS,
        PARTIAL_MATCH_MISSING_AND_EXTRA_COMPONENTS
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @EqualsAndHashCode
    public static class ComplexMatch<T> {
        private String complexAc;
        private MatchType matchType;
        private float similarity;
        private Collection<ComplexComponent> matchingProteins;
        private Collection<ComplexComponent> extraProteins;
        private Collection<String> missingProteins;
        private T complex;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @EqualsAndHashCode
    public static class ComplexComponent {
        private String primaryId;
        private Collection<String> otherIds;
    }
}
