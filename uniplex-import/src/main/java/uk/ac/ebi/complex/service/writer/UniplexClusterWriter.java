package uk.ac.ebi.complex.service.writer;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.model.UniplexCluster;

@SuperBuilder
public class UniplexClusterWriter extends ComplexFileWriter<Integer, UniplexCluster> {

    @Override
    protected String[] complexToStringArray(UniplexCluster complex) {
        String clusterIds = String.join(" ", complex.getComplexIds());
        String clusterConfidence = complex.getConfidence().toString();
        String proteinIds = String.join(" ", complex.getProteinIds());
        return new String[]{clusterIds, clusterConfidence, proteinIds};
    }
}
