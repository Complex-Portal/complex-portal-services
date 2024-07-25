package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.Data;
import lombok.extern.java.Log;
import uk.ac.ebi.complex.service.config.ComplexServiceConfiguration;
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.complex.service.service.UniProtMappingService;
import uk.ac.ebi.complex.service.logging.FailedWriter;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Log
@Data
public class UniplexClusterReader {

    private final UniProtMappingService uniProtMappingService = new UniProtMappingService();
    private ComplexServiceConfiguration config;
    private FailedWriter ignoredReportWriter;

    public UniplexClusterReader(ComplexServiceConfiguration config) throws IOException {
        this.config = config;
        this.initialiseReportWriters();
    }

    public Collection<UniplexCluster> readClustersFromFile() throws IOException {
        File inputFile = new File(config.getInputFileName());
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(config.getSeparator().charAt(0)).build())
                .build();


        if (config.isHeader()) {
            csvReader.skip(1);
        }

        List<UniplexCluster> clusters = new ArrayList<>();
        csvReader.forEach(csvLine -> clusters.add(clusterFromStringArray(csvLine)));
        csvReader.close();

        return cleanUniprotACs(clusters);
    }

    private List<UniplexCluster> cleanUniprotACs(List<UniplexCluster> clusters) throws IOException {
        Set<String> identifiers = clusters.stream()
                .flatMap(c -> c.getUniprotAcs().stream())
                .collect(Collectors.toSet());

        Map<String, List<String>> uniprotMapping = uniProtMappingService.mapIds(identifiers);

        List<UniplexCluster> mappedClusters = new ArrayList<>();

        for (UniplexCluster cluster : clusters) {
            Collection<String> ids = cluster.getUniprotAcs();
            List<String> mappedIds = new ArrayList<>();
            Map<String, String> problems = new HashMap<>();
            for (String id : ids) {
                List<String> termMapping = uniprotMapping.get(id);
                if (termMapping == null) {
                    problems.put(id, String.format("%s has never existed", id));
                } else if (termMapping.size() != 1) {
                    if (termMapping.isEmpty()) {
                        problems.put(id, String.format("%s has been deleted", id));
                    } else {
                        problems.put(id, String.format("%s has an ambiguous mapping to %s", id, String.join(" and ", termMapping)));
                    }
                } else {
                    String mappedId = termMapping.get(0);
                    mappedIds.add(mappedId);
                }
            }
            if (problems.isEmpty()) {
                cluster.setUniprotAcs(mappedIds);
                mappedClusters.add(cluster);
            } else {
                ignoredReportWriter.write(cluster.getClusterIds(), cluster.getUniprotAcs(), problems.keySet(), problems.values());
            }
        }

        return mappedClusters;
    }

    private UniplexCluster clusterFromStringArray(String[] csvLine) {
        String clusterId = csvLine[0];
        String clusterConfidence = csvLine[1];
        String[] uniprotAcs = csvLine[2].split(" ");

        return new UniplexCluster(
                Collections.singletonList(clusterId),
                Integer.parseInt(clusterConfidence),
                Arrays.stream(uniprotAcs)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()));
    }

    private void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(config.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + config.getReportDirectory());
        }

        this.ignoredReportWriter = new FailedWriter(new File(reportDirectory, "ignored.tsv"), "\t", true);
    }
}
