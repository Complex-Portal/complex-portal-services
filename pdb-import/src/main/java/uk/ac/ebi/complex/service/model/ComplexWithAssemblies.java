package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ComplexWithAssemblies {
    private String complexId;
    private Collection<String> assembliesFromFile;
//    private Collection<String> assembliesWithSameProteins;
}
