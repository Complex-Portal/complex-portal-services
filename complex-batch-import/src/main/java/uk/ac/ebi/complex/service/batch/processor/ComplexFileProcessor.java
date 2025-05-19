package uk.ac.ebi.complex.service.batch.processor;

import lombok.extern.log4j.Log4j;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.logging.FailedWriter;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;
import uk.ac.ebi.complex.service.batch.reader.ComplexFileReader;
import uk.ac.ebi.complex.service.batch.writer.ComplexFileWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Log4j
public abstract class ComplexFileProcessor<T, R extends ComplexToImport<T>> {

    private final static int NUMBER_OF_COMPONENTS_LIMIT = 100;
    private final static String COMPLEX_TOO_LARGE_ERROR = "Complex is too large, number of components = %d";

    private final FileConfiguration fileConfiguration;
    private final ComplexFileReader<T, R> complexFileReader;
    private final ComplexFileWriter<T, R> complexFileWriter;

    private FailedWriter ignoredReportWriter;

    public ComplexFileProcessor(FileConfiguration fileConfiguration,
                                ComplexFileReader<T, R> complexFileReader,
                                ComplexFileWriter<T, R> complexFileWriter) throws IOException {

        this.fileConfiguration = fileConfiguration;
        this.complexFileReader = complexFileReader;
        this.complexFileWriter = complexFileWriter;
        initialiseReportWriters();
    }

    public void processFile() throws IOException {
        // First delete the temp file if it exists
        File tempOutputFile = fileConfiguration.outputPath().toFile();
        if (tempOutputFile.exists()) {
            log.info("Deleting file " + fileConfiguration.getOutputFileName() + "...");
            tempOutputFile.delete();
        }

        log.info("Reading input file...");
        File inputFile = new File(fileConfiguration.getInputFileName());
        Collection<R> complexes = complexFileReader.readComplexesFromFile(inputFile);
        log.info("Filtering out complexes with large number of components...");
        Collection<R> complexesWithoutLargeOnes = filterOutLargeComplexes(complexes);
        log.info("Cleaning all UniProt ACs...");
        Collection<R> complexesWithCleanUniprotAcs = cleanUniprotACs(complexesWithoutLargeOnes);
        log.info("Checking duplicates...");
        Collection<R> complexesWithoutDuplicates = mergeDuplicateComplexes(complexesWithCleanUniprotAcs);
        log.info("Writing output file...");
        complexFileWriter.writeComplexesToFile(complexesWithoutDuplicates);
        // TODO: filter out low confidence complexes
    }

    protected abstract Map<String, List<UniprotProtein>> mapToUniprotProteins(Collection<R> complexes);

    protected abstract R mergeComplexes(R complexA, R complexB);

    private Collection<R> mergeDuplicateComplexes(Collection<R> complexes) {
        // We convert the list of complexes to a map, indexed by the sorted uniprot ACs of each complex to
        // remove duplicates.
        return complexes.stream()
                .collect(Collectors.toMap(
                        complex -> String.join(",", complex.getProteinIds()),
                        complex -> complex,
                        this::mergeComplexes))
                .values();
    }

    private Collection<R> filterOutLargeComplexes(Collection<R> complexes) throws IOException {
        List<R> filteredComplexes = new ArrayList<>();
        for (R complex : complexes) {
            int numberOfComponents = complex.getProteinIds().size();
            if (numberOfComponents > NUMBER_OF_COMPONENTS_LIMIT) {
                ignoredReportWriter.write(
                        complex.getComplexIds(),
                        complex.getProteinIds(),
                        new ArrayList<>(),
                        List.of(String.format(COMPLEX_TOO_LARGE_ERROR, numberOfComponents)));
            } else {
                filteredComplexes.add(complex);
            }
        }
        return filteredComplexes;
    }

    private Collection<R> cleanUniprotACs(Collection<R> complexes) throws IOException {
        Map<String, List<UniprotProtein>> uniprotMapping =mapToUniprotProteins(complexes);

        List<R> mappedComplexes = new ArrayList<>();

        for (R complex : complexes) {
            Collection<String> ids = complex.getProteinIds();
            List<String> mappedIds = new ArrayList<>();
            Map<String, String> problems = new HashMap<>();
            for (String id : ids) {
                List<UniprotProtein> termMapping = uniprotMapping.get(id);
                if (termMapping == null) {
                    problems.put(id, String.format("%s has never existed", id));
                } else if (termMapping.size() != 1) {
                    if (termMapping.isEmpty()) {
                        problems.put(id, String.format("%s has been deleted", id));
                    } else {
                        problems.put(id, String.format(
                                "%s has an ambiguous mapping to %s",
                                id,
                                termMapping.stream().map(UniprotProtein::getProteinAc).collect(Collectors.joining(" and "))));;
                    }
                } else {
                    String mappedId = termMapping.get(0).getProteinAc();
                    mappedIds.add(mappedId);
                }
            }
            if (problems.isEmpty()) {
                complex.setProteinIds(mappedIds);
                mappedComplexes.add(complex);
            } else {
                ignoredReportWriter.write(complex.getComplexIds(), complex.getProteinIds(), problems.keySet(), problems.values());
            }
        }

        return mappedComplexes;
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
}
