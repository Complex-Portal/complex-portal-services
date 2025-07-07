package uk.ac.ebi.complex.service.interactions.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class IntactProteinPairScore {

    String proteinA;
    String proteinB;
    Double score;
}
