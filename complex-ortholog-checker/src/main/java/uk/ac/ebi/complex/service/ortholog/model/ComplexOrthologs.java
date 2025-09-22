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
    private ComplexWithXrefs inputComplex;
    private Collection<ComplexWithXrefs> outputComplexes;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ComplexWithXrefs {
        private String complexId;
        private String complexName;
        private boolean predicted;
        private Collection<String> cellularComponents;
    }
}
