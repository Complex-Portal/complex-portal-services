package uk.ac.ebi.complex.service.interactions.model;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class ComplexIntactCoverage {

    String complexAc;
    List<ProteinPairScore> proteinPairScores;

    @Value
    @Builder
    public static class ProteinPairScore {

        String proteinA;
        String proteinB;
        Double intactScore;
    }
}
