package uk.ac.ebi.complex.service.covariation.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class ProteinCovariation {
    private List<String> proteinA;
    private List<String> proteinB;
    private Double probability;
}
