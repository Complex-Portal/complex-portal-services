package uk.ac.ebi.complex.service.covariation.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class ProteinPairCovariation {
    private String proteinA;
    private String proteinB;
    private Double probability;
}
