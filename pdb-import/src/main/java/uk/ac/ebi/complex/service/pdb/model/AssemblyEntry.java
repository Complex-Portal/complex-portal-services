package uk.ac.ebi.complex.service.pdb.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;

import java.util.Collection;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AssemblyEntry {
    private Collection<UniprotProtein> proteins;
    private Collection<String> assemblies;
    private Collection<String> complexIds;
}
