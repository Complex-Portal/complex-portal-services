package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ComplexCluster {
    private Collection<String> complexAcs;
    private Set<String> matchingProteins;
    private Set<String> missingProteins;
}
