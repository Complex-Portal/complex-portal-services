package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UniplexComplex {
    private UniplexCluster cluster;
    private IntactComplex existingComplex;
}
