package uk.ac.ebi.complex.service.pdb.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ComplexWithAssemblies {
    private String complexId;
    private boolean predicted;
    private Collection<String> assembliesFromFile;
    private Collection<String> assembliesWithSameProteins;
}
