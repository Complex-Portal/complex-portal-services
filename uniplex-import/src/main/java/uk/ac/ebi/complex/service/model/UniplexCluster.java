package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UniplexCluster {
    private Collection<String> clusterIds;
    private Integer clusterConfidence;
    private Collection<String> uniprotAcs;
}
