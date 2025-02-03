package uk.ac.ebi.complex.service.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.util.Collection;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ComplexWithMatches<T, R extends ComplexToImport<T>> {
    private R complexToImport;
    private Collection<IntactComplex> complexesWithExactMatch;
    private Collection<IntactComplex> complexesToAddSubsetXref;
    private Collection<List<IntactComplex>> complexesToAddComplexClusterXref;
}
