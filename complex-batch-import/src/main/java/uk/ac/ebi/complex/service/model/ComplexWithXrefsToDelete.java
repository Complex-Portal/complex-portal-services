package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ComplexWithXrefsToDelete {
    private IntactComplex complex;
    private List<Xref> identityXrefs;
    private List<Xref> subsetXrefs;
    private List<Xref> complexClusterXrefs;
}
