package uk.ac.ebi.complex.service.uniplex.processor;

import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.processor.ComplexFileProcessor;
import uk.ac.ebi.complex.service.uniplex.model.UniplexCluster;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;
import uk.ac.ebi.complex.service.uniplex.reader.UniplexClusterReader;
import uk.ac.ebi.complex.service.batch.service.UniProtMappingService;
import uk.ac.ebi.complex.service.uniplex.writer.UniplexClusterWriter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j
@Component
public class UniplexFileProcessor extends ComplexFileProcessor<Integer, UniplexCluster> {

    private final UniProtMappingService uniProtMappingService;

    public UniplexFileProcessor(FileConfiguration fileConfiguration,
                                UniplexClusterReader uniplexClusterReader,
                                UniplexClusterWriter uniplexClusterWriter,
                                UniProtMappingService uniProtMappingService) throws IOException {

        super(fileConfiguration, uniplexClusterReader, uniplexClusterWriter);
        this.uniProtMappingService = uniProtMappingService;
    }

    @Override
    protected Map<String, List<UniprotProtein>> mapToUniprotProteins(Collection<UniplexCluster> complexes) {
        Set<String> identifiers = complexes.stream()
                .flatMap(c -> c.getProteinIds().stream())
                .collect(Collectors.toSet());

        return uniProtMappingService.mapIds(identifiers);
    }

    @Override
    protected UniplexCluster mergeComplexes(UniplexCluster complexA, UniplexCluster complexB) {
        log.info("Duplicates cluster found: " +
                String.join(",", complexA.getComplexIds()) +
                " and " +
                String.join(",", complexB.getComplexIds()));

        List<String> clusterIds = Stream.concat(complexA.getComplexIds().stream(), complexB.getComplexIds().stream())
                .distinct()
                .collect(Collectors.toList());
        Integer clusterConfidence = Integer.max(complexA.getConfidence(), complexB.getConfidence());

        return UniplexCluster.builder()
                .complexIds(clusterIds)
                .confidence(clusterConfidence)
                .proteinIds(complexA.getProteinIds())
                .build();
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
                UniplexClusterReader.builder().fileConfiguration(c).build(),
                UniplexClusterWriter.builder().fileConfiguration(c).build(),
                new UniProtMappingService());
        uniplexFileProcessor.processFile();
    }
}
