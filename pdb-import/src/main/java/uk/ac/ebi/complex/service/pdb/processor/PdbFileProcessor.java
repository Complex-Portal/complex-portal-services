package uk.ac.ebi.complex.service.pdb.processor;

import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.finder.ComplexFinder;
import uk.ac.ebi.complex.service.finder.ComplexFinderOptions;
import uk.ac.ebi.complex.service.finder.ComplexFinderResult;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.logging.FailedWriter;
import uk.ac.ebi.complex.service.pdb.model.AssemblyEntry;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;
import uk.ac.ebi.complex.service.pdb.reader.PdbAssembliesFileReader;
import uk.ac.ebi.complex.service.batch.service.UniProtMappingService;
import uk.ac.ebi.complex.service.pdb.writer.PdbAssembliesFileWriter;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

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
public class PdbFileProcessor {

    private final FileConfiguration fileConfiguration;
    private final PdbAssembliesFileReader pdbAssembliesFileReader;
    private final PdbAssembliesFileWriter pdbAssembliesFileWriter;
    private final UniProtMappingService uniProtMappingService;
    private final ComplexFinder complexFinder;

    private FailedWriter ignoredReportWriter;
    private long mergedAssemblies;

    public PdbFileProcessor(FileConfiguration fileConfiguration,
                            PdbAssembliesFileReader pdbAssembliesFileReader,
                            PdbAssembliesFileWriter pdbAssembliesFileWriter,
                            UniProtMappingService uniProtMappingService,
                            ComplexFinder complexFinder) throws IOException {

        this.fileConfiguration = fileConfiguration;
        this.pdbAssembliesFileReader = pdbAssembliesFileReader;
        this.pdbAssembliesFileWriter = pdbAssembliesFileWriter;
        this.uniProtMappingService = uniProtMappingService;
        this.complexFinder = complexFinder;
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
        Collection<AssemblyEntry> assemblies = pdbAssembliesFileReader.readAssembliesFromFile(inputFile);
        log.info("Number of assemblies read = " + assemblies.size());
        log.info("Cleaning all UniProt ACs...");
        Collection<AssemblyEntry> assembliesWithCleanUniprotAcs = cleanUniprotACs(assemblies);
        log.info("Number of assemblies after cleaning uniprot ACs = " + assembliesWithCleanUniprotAcs.size());
        log.info("Checking duplicates...");
        Collection<AssemblyEntry> assembliesWithoutDuplicates = mergeDuplicates(assembliesWithCleanUniprotAcs);
        log.info("Duplicates found = " + mergedAssemblies);
        log.info("Number of assemblies after merging duplicates = " + assembliesWithoutDuplicates.size());
//        log.info("Finding matches...");
//        Collection<AssemblyEntry> assembliesWithMatches = findMatches(assembliesWithoutDuplicates);
//        log.info("Number of assemblies after finding matches = " + assembliesWithMatches.size());
        log.info("Writing output file...");
        pdbAssembliesFileWriter.writeAssembliesToFile(assembliesWithoutDuplicates);
        // TODO: filter out low confidence complexes
    }

    private Collection<AssemblyEntry> findMatches(Collection<AssemblyEntry> assemblies) {
        List<AssemblyEntry> assembliesWithMatches = new ArrayList<>();
        long processed = 0;
        for (AssemblyEntry assemblyEntry : assemblies) {
            List<String> complexIds = new ArrayList<>(assemblyEntry.getComplexIds());
            if (!assemblyEntry.getProteins().isEmpty()) {
                ComplexFinderResult<IntactComplex> complexFinderResult = complexFinder.findComplexWithMatchingProteins(
                        assemblyEntry.getProteins().stream().map(UniprotProtein::getProteinAc).collect(Collectors.toSet()),
                        ComplexFinderOptions.builder()
                                .checkPredictedComplexes(true)
                                .checkAnyStatusForExactMatches(false)
                                .checkPartialMatches(false)
                                .build());

                if (!complexFinderResult.getExactMatches().isEmpty()) {
                    for (ComplexFinderResult.ExactMatch<IntactComplex> exactMatch : complexFinderResult.getExactMatches()) {
                        if (!complexIds.contains(exactMatch.getComplexAc())) {
                            complexIds.add(exactMatch.getComplexAc());
                        }
                    }
                }
            }
            if (!complexIds.isEmpty()) {
                assemblyEntry.setComplexIds(complexIds);
                assembliesWithMatches.add(assemblyEntry);
            }
            processed++;
            if (processed % 100 == 0) {
                log.info("Finding matches... " + processed + " / " + assemblies.size());
            }
        }
        return assembliesWithMatches;
    }

    private Collection<AssemblyEntry> mergeDuplicates(Collection<AssemblyEntry> assemblies) {
        mergedAssemblies = 0;
        // We convert the list of complexes to a map, indexed by the sorted uniprot ACs of each complex to
        // remove duplicates.
        return assemblies.stream()
                .collect(Collectors.toMap(
                        this::getAssemblyEntryKey,
                        assembly -> assembly,
                        this::mergeAssemblies))
                .values();
    }

    private String getAssemblyEntryKey(AssemblyEntry assemblyEntry) {
        return String.join(",", assemblyEntry.getComplexIds()) +
                "_" +
                assemblyEntry.getProteins().stream().map(UniprotProtein::getProteinAc).collect(Collectors.joining(","));
    }

    private AssemblyEntry mergeAssemblies(AssemblyEntry assemblyA, AssemblyEntry assemblyB) {
//        log.info("Duplicates assembly found: " +
//                String.join(",", assemblyA.getAssemblies()) +
//                " and " +
//                String.join(",", assemblyB.getAssemblies()));

        List<String> assemblies = Stream.concat(assemblyA.getAssemblies().stream(), assemblyB.getAssemblies().stream())
                .distinct()
                .collect(Collectors.toList());

        mergedAssemblies++;

        return AssemblyEntry.builder()
                .assemblies(assemblies)
                .complexIds(assemblyA.getComplexIds())
                .proteins(assemblyA.getProteins())
                .build();

    }

    private Collection<AssemblyEntry> cleanUniprotACs(Collection<AssemblyEntry> assemblies) throws IOException {
        Set<String> identifiers = assemblies.stream()
                .flatMap(a -> a.getProteins().stream())
                .map(UniprotProtein::getProteinAc)
                .collect(Collectors.toSet());

        Map<String, List<UniprotProtein>> uniprotMapping = uniProtMappingService.mapIds(identifiers);

        List<AssemblyEntry> mappedAssemblies = new ArrayList<>();

        for (AssemblyEntry assembly : assemblies) {
            Collection<UniprotProtein> proteins = assembly.getProteins();
            List<UniprotProtein> mappedProteins = new ArrayList<>();
            Map<String, String> problems = new HashMap<>();
            for (UniprotProtein protein : proteins) {
                String id = protein.getProteinAc();
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
                    mappedProteins.add(termMapping.get(0));
                }
            }
            if (proteins.isEmpty()) {
                problems.put(String.join(" ", assembly.getAssemblies()), "No proteins could be mapped for this assembly");
            }

            if (problems.isEmpty()) {
                assembly.setProteins(mappedProteins);
                mappedAssemblies.add(assembly);
            } else if (!assembly.getComplexIds().isEmpty()) {
                assembly.setProteins(List.of());
                mappedAssemblies.add(assembly);
            } else {
                ignoredReportWriter.write(
                        assembly.getAssemblies(),
                        assembly.getProteins().stream().map(UniprotProtein::getProteinAc).collect(Collectors.toSet()),
                        problems.keySet(),
                        problems.values());
            }
        }

        return mappedAssemblies;
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
