package uk.ac.ebi.complex.service.ortholog.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ComplexOrthologs {
    private String inputComplexId;
    private Collection<String> outputComplexIds;
}
