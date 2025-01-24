package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UniprotProtein {
    private String proteinAc;
    private String proteinName;
    private String geneName;
    private Integer organism;
    private boolean reviewed;
}
