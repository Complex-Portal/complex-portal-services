package uk.ac.ebi.complex.service.processor;

import lombok.AllArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.complex.service.reader.UniplexClusterReader;
import uk.ac.ebi.complex.service.writer.UniplexClusterWriter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class UniplexFileProcessor {

    private static final Log LOG = LogFactory.getLog(UniplexFileProcessor.class);

    private final String inputFileName;
    private final String outputFileName;
    private final UniplexClusterReader uniplexClusterReader;
    private final UniplexClusterWriter uniplexClusterWriter;

    public void processFile() throws IOException {
        // First delete the temp file if it exists
        File tempOutputFile = new File(outputFileName);
        if (tempOutputFile.exists()) {
            LOG.info("Deleting file " + outputFileName + "...");
            tempOutputFile.delete();
        }

        LOG.info("Reading Uniplex file...");
        Collection<UniplexCluster> clusters = uniplexClusterReader.readClustersFromFile(inputFileName);
        LOG.info("Checking duplicates...");
        Collection<UniplexCluster> clustersWithoutDuplicates = mergeDuplicateClusters(clusters);
        LOG.info("Writing output file...");
        uniplexClusterWriter.writeClustersToFile(clustersWithoutDuplicates, outputFileName);
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
        LOG.info("Duplicates cluster found: " +
                String.join(",", clusterA.getClusterIds()) +
                " and " +
                String.join(",", clusterB.getClusterIds()));

        List<String> clusterIds = Stream.concat(clusterA.getClusterIds().stream(), clusterB.getClusterIds().stream())
                .distinct()
                .collect(Collectors.toList());
        Integer clusterConfidence = Integer.max(clusterA.getClusterConfidence(), clusterB.getClusterConfidence());

        return new UniplexCluster(clusterIds, clusterConfidence, clusterA.getUniprotAcs());
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("Usage: UniplexFileProcessor <input_file_name> <output_file_name> <separator> <header>");
            System.exit(1);
        }

        String inputFileName = args[0];
        String outputFileName = args[1];
        String separator = args[2];
        boolean header = Boolean.parseBoolean(args[3]);

        UniplexFileProcessor uniplexFileProcessor = new UniplexFileProcessor(
                inputFileName,
                outputFileName,
                new UniplexClusterReader(separator, header),
                new UniplexClusterWriter(separator, header));

        uniplexFileProcessor.processFile();
    }
}
