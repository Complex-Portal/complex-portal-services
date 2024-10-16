package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ComplexWithAssemblyXrefs {
    private String complexId;
    List<String> xrefsToAdd;
    List<InteractorXref> xrefsToUpdate;
    List<InteractorXref> xrefsToRemove;
}
