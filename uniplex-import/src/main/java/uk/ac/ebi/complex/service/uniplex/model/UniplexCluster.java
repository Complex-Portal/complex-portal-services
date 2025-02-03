package uk.ac.ebi.complex.service.uniplex.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;

@Data
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class UniplexCluster extends ComplexToImport<Integer> {
}
