package uk.ac.ebi.complex.service.uniplex.reader;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.batch.reader.ComplexFileReader;
import uk.ac.ebi.complex.service.uniplex.model.UniplexCluster;

import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
public class UniplexClusterReader extends ComplexFileReader<Integer, UniplexCluster> {

    @Override
    protected UniplexCluster complexFromStringArray(String[] csvLine) {
        String[] clusterIds = csvLine[0].split(" ");
        String clusterConfidence = csvLine[1];
        String[] uniprotAcs = csvLine[2].split(" ");

        return UniplexCluster.builder()
                .complexIds(Arrays.stream(clusterIds)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()))
                .confidence(Integer.parseInt(clusterConfidence))
                .proteinIds(Arrays.stream(uniprotAcs)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()))
                .build();
    }
}
