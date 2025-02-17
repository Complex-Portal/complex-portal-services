package uk.ac.ebi.complex.service.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Collection;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class ComplexToImport<T> {
    private Collection<String> complexIds;
    private T confidence;
    private Collection<String> proteinIds;
}
