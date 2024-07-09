package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.AllArgsConstructor;
import uk.ac.ebi.complex.service.model.UniplexCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class UniplexClusterReader {

    private final String separator;
    private final boolean header;

    public Collection<UniplexCluster> readClustersFromFile(String inputFileName) throws IOException {
        File inputFile = new File(inputFileName);
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(separator.charAt(0)).build())
                .build();

        if (header) {
            csvReader.skip(1);
        }

        List<UniplexCluster> clusters = new ArrayList<>();
        csvReader.forEach(csvLine -> clusters.add(clusterFromStringArray(csvLine)));
        csvReader.close();

        return clusters;
    }

    private UniplexCluster clusterFromStringArray(String[] csvLine) {
        String clusterId = csvLine[0];
        String clusterConfidence = csvLine[1];
        String[] uniprotAcs = csvLine[2].split(" ");
        // TODO: for each uniprotAc, call UniProt API to get the latest canonical id

        return new UniplexCluster(
                Collections.singletonList(clusterId),
                Integer.parseInt(clusterConfidence),
                Arrays.stream(uniprotAcs)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()));
    }
}
