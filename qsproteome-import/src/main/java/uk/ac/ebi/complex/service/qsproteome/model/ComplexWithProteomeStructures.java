package uk.ac.ebi.complex.service.qsproteome.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ComplexWithProteomeStructures {
    private String complexId;
    private Collection<String> qsProteomeIds;
}
