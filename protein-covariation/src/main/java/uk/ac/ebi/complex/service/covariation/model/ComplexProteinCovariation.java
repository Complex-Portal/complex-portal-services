package uk.ac.ebi.complex.service.covariation.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ComplexProteinCovariation {

    private double covariationCoverage;
    private List<String> proteinIds;
    private List<List<Double>> proteinCovariations;
}
