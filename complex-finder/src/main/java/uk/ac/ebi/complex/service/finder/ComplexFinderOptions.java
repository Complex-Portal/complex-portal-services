package uk.ac.ebi.complex.service.finder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ComplexFinderOptions {
    private boolean checkPredictedComplexes;
    private boolean checkAnyStatusForExactMatches;
    private boolean checkPartialMatches;
}
