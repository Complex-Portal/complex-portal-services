package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class UniplexComplex {
    private UniplexCluster cluster;
    private IntactComplex existingComplex;
}
