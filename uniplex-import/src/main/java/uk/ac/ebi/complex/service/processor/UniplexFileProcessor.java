package uk.ac.ebi.complex.service.processor;

import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.logging.FailedWriter;
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.complex.service.reader.UniplexClusterReader;
import uk.ac.ebi.complex.service.service.UniProtMappingService;
import uk.ac.ebi.complex.service.writer.UniplexClusterWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j
@Component
public class UniplexFileProcessor {

    private final FileConfiguration fileConfiguration;
    private final UniplexClusterReader uniplexClusterReader;
    private final UniplexClusterWriter uniplexClusterWriter;
    private final UniProtMappingService uniProtMappingService;

    private FailedWriter ignoredReportWriter;

    public UniplexFileProcessor(FileConfiguration fileConfiguration,
                                UniplexClusterReader uniplexClusterReader,
                                UniplexClusterWriter uniplexClusterWriter,
                                UniProtMappingService uniProtMappingService) throws IOException {

        this.fileConfiguration = fileConfiguration;
        this.uniplexClusterReader = uniplexClusterReader;
        this.uniplexClusterWriter = uniplexClusterWriter;
        this.uniProtMappingService = uniProtMappingService;
        initialiseReportWriters();
    }

    public void processFile() throws IOException {
        // First delete the temp file if it exists
        File tempOutputFile = fileConfiguration.outputPath().toFile();
        if (tempOutputFile.exists()) {
            log.info("Deleting file " + fileConfiguration.getOutputFileName() + "...");
            tempOutputFile.delete();
        }

        log.info("Reading Uniplex file...");
        Collection<UniplexCluster> clusters = uniplexClusterReader.readClustersFromFile();
        log.info("Cleaning all UniProt ACs...");
        Collection<UniplexCluster> clustersWithCleanUniprotAcs = cleanUniprotACs(clusters);
        log.info("Checking duplicates...");
        Collection<UniplexCluster> clustersWithoutDuplicates = mergeDuplicateClusters(clustersWithCleanUniprotAcs);
        log.info("Writing output file...");
        uniplexClusterWriter.writeClustersToFile(clustersWithoutDuplicates);
        // TODO: filter out low confidence clusters
    }

    private Collection<UniplexCluster> mergeDuplicateClusters(Collection<UniplexCluster> clusters) {
        // We convert the list of clusters to a map, indexed by the sorted uniprot ACs of each cluster to
        // remove duplicates.
        return clusters.stream()
                .collect(Collectors.toMap(
                        uniplexCluster -> String.join(",", uniplexCluster.getUniprotAcs()),
                        uniplexCluster -> uniplexCluster,
                        this::mergeClusters))
                .values();
    }

    private UniplexCluster mergeClusters(UniplexCluster clusterA, UniplexCluster clusterB) {
        log.info("Duplicates cluster found: " +
                String.join(",", clusterA.getClusterIds()) +
                " and " +
                String.join(",", clusterB.getClusterIds()));

        List<String> clusterIds = Stream.concat(clusterA.getClusterIds().stream(), clusterB.getClusterIds().stream())
                .distinct()
                .collect(Collectors.toList());
        Integer clusterConfidence = Integer.max(clusterA.getClusterConfidence(), clusterB.getClusterConfidence());

        return new UniplexCluster(clusterIds, clusterConfidence, clusterA.getUniprotAcs());
    }

    private List<UniplexCluster> cleanUniprotACs(Collection<UniplexCluster> clusters) throws IOException {
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

    private void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + fileConfiguration.getReportDirectory());
        }

        this.ignoredReportWriter = new FailedWriter(
                new File(reportDirectory, "ignored" + fileConfiguration.getExtension()),
                fileConfiguration.getSeparator(),
                fileConfiguration.isHeader());
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            System.err.println("Usage: UniplexFileProcessor <input_file_name> <output_file_name> <report_directory> <separator> <header>");
            System.exit(1);
        }

        FileConfiguration c = FileConfiguration.builder()
                .inputFileName(args[0])
                .outputFileName(args[1])
                .reportDirectory(args[2])
                .separator(args[3])
                .header(Boolean.parseBoolean(args[4]))
                .build();

        UniplexFileProcessor uniplexFileProcessor = new UniplexFileProcessor(
                c,
                new UniplexClusterReader(c),
                new UniplexClusterWriter(c),
                new UniProtMappingService());
        uniplexFileProcessor.processFile();
    }
}
