package uk.ac.ebi.complex.service.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collection;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
public class UniplexCluster {
    private Collection<String> clusterIds;
    private Integer clusterConfidence;
    private Collection<String> uniprotAcs;
}
