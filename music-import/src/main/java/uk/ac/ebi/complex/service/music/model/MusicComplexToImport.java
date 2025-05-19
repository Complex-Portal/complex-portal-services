package uk.ac.ebi.complex.service.music.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class MusicComplexToImport extends ComplexToImport<Double> {
    private String name;
}
