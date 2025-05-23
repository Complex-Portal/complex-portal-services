package uk.ac.ebi.complex.service.music.processor;

import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.processor.ComplexFileProcessor;
import uk.ac.ebi.complex.service.music.config.MusicImportAppProperties;
import uk.ac.ebi.complex.service.music.model.MusicComplexToImport;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;
import uk.ac.ebi.complex.service.music.reader.MusicComplexReader;
import uk.ac.ebi.complex.service.batch.service.UniProtMappingService;
import uk.ac.ebi.complex.service.music.writer.MusicComplexWriter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j
@Component
public class MusicFileProcessor extends ComplexFileProcessor<Double, MusicComplexToImport> {

    private final UniProtMappingService uniProtMappingService;

    public MusicFileProcessor(FileConfiguration fileConfiguration,
                              MusicComplexReader musicComplexReader,
                              MusicComplexWriter musicComplexWriter,
                              UniProtMappingService uniProtMappingService) throws IOException {

        super(fileConfiguration, musicComplexReader, musicComplexWriter);
        this.uniProtMappingService = uniProtMappingService;
    }

    @Override
    protected Map<String, List<UniprotProtein>> mapToUniprotProteins(Collection<MusicComplexToImport> complexes) {
        Set<String> identifiers = complexes.stream()
                .flatMap(c -> c.getProteinIds().stream())
                .collect(Collectors.toSet());

        return uniProtMappingService.mapGenes(identifiers);
    }

    @Override
    protected MusicComplexToImport mergeComplexes(MusicComplexToImport complexA, MusicComplexToImport complexB) {
        log.info("Duplicates complex found: " +
                String.join(",", complexA.getComplexIds()) +
                " and " +
                String.join(",", complexB.getComplexIds()));

        String name = complexA.getName() != null ? complexA.getName() : complexB.getName();
        List<String> ids = Stream.concat(complexA.getComplexIds().stream(), complexB.getComplexIds().stream())
                .distinct()
                .collect(Collectors.toList());

        Double confidence = null;
        if (complexA.getConfidence() != null) {
            if (complexB.getConfidence() != null) {
                confidence = Double.max(complexA.getConfidence(), complexB.getConfidence());
            } else {
                confidence = complexA.getConfidence();
            }
        } else if (complexB.getConfidence() != null) {
            confidence = complexB.getConfidence();
        }

        return MusicComplexToImport.builder()
                .name(name)
                .complexIds(ids)
                .confidence(confidence)
                .proteinIds(complexA.getProteinIds())
                .build();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            System.err.println("Usage: MusicFileProcessor <input_file_name> <output_file_name> <report_directory> <separator> <header>");
            System.exit(1);
        }

        FileConfiguration c = FileConfiguration.builder()
                .inputFileName(args[0])
                .outputFileName(args[1])
                .reportDirectory(args[2])
                .separator(args[3])
                .header(Boolean.parseBoolean(args[4]))
                .build();

        MusicImportAppProperties musicImportAppProperties = MusicImportAppProperties.builder()
                .inputFileFieldsString("ids,proteins,confidence")
                .build();

        MusicFileProcessor musicFileProcessor = new MusicFileProcessor(
                c,
                MusicComplexReader.builder()
                        .fileConfiguration(c)
                        .musicImportAppProperties(musicImportAppProperties)
                        .build(),
                MusicComplexWriter.builder()
                        .fileConfiguration(c)
                        .musicImportAppProperties(musicImportAppProperties)
                        .build(),
                new UniProtMappingService());
        musicFileProcessor.processFile();
    }
}
