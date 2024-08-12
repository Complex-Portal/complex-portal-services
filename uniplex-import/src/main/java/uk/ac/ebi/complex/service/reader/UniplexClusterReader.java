package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.UniplexCluster;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Log4j
@Component
@RequiredArgsConstructor
public class UniplexClusterReader {

    private final FileConfiguration fileConfiguration;

    private int lineCounter = 0;

    public Collection<UniplexCluster> readClustersFromFile() throws IOException {
        File inputFile = new File(fileConfiguration.getInputFileName());
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();


        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
            lineCounter++;
        }

        List<UniplexCluster> clusters = new ArrayList<>();
        csvReader.forEach(csvLine -> clusters.add(clusterFromStringArray(csvLine)));
        csvReader.close();

        return clusters;
    }

    private UniplexCluster clusterFromStringArray(String[] csvLine) {
        try {
            String clusterId = csvLine[0];
            String clusterConfidence = csvLine[1];
            String[] uniprotAcs = csvLine[2].split(" ");

            lineCounter++;

            return new UniplexCluster(
                    Collections.singletonList(clusterId),
                    Integer.parseInt(clusterConfidence),
                    Arrays.stream(uniprotAcs)
                            .sorted()
                            .distinct()
                            .collect(Collectors.toList()));
        } catch (java.lang.ArrayIndexOutOfBoundsException e) {
            log.error("Error reading line " + lineCounter + ": " + String.join(",", csvLine), e);
            throw e;
        }
    }
}
