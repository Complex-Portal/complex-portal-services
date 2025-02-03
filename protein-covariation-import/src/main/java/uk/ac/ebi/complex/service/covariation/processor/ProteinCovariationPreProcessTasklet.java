package uk.ac.ebi.complex.service.covariation.processor;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.repeat.RepeatStatus;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.Interactor;
import psidev.psi.mi.jami.model.Participant;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.covariation.logging.ProteinFailedWriter;
import uk.ac.ebi.complex.service.batch.model.UniprotProtein;
import uk.ac.ebi.complex.service.covariation.reader.ProteinIdsReader;
import uk.ac.ebi.complex.service.batch.service.UniProtMappingService;
import uk.ac.ebi.intact.jami.service.ComplexService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@RequiredArgsConstructor
public class ProteinCovariationPreProcessTasklet implements Tasklet {

    private final ComplexService complexService;
    private final UniProtMappingService uniProtMappingService;
    private final ProteinIdsReader proteinIdsReader;
    private final FileConfiguration fileConfiguration;

    private ProteinFailedWriter ignoredReportWriter;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        initialiseOutputDirectory();
        loadAndSaveAllComplexesAndProteinsInIntact();
        generateAndSaveUniprotMapping();
        return RepeatStatus.FINISHED;
    }

    private void loadAndSaveAllComplexesAndProteinsInIntact() throws IOException {
        log.info("Reading all complexes and proteins");

        Set<String> proteinsInIntact = new HashSet<>();
        Map<String, Set<String>> complexesInIntact = new HashMap<>();
        long count = 0;

        Iterator<Complex> complexIterator = complexService.iterateAll();
        while (complexIterator.hasNext()) {
            count++;
            if (count % 500 == 0) {
                log.info("Processed " + count + " complexes");
            }
            Complex complex = complexIterator.next();
            Set<String> participantIds = new HashSet<>();
            for (Participant participant : complex.getParticipants()) {
                if (participant.getInteractor() != null) {
                    Interactor interactor = participant.getInteractor();
                    if (interactor.getPreferredIdentifier() != null) {
                        Xref xref = interactor.getPreferredIdentifier();
                        if (xref.getId() != null && !xref.getId().isEmpty()) {
                            participantIds.add(xref.getId());
                        }
                    }
                }
            }
            complexesInIntact.putIfAbsent(complex.getPreferredIdentifier().getId(), participantIds);
            proteinsInIntact.addAll(participantIds);
        }

        log.info("Reading all complexes and proteins - DONE");
        log.info("Saving all complexes");

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        File complexesFile = new File(reportDirectory, "complexes" + fileConfiguration.getExtension());
        BufferedWriter writer = new BufferedWriter(new FileWriter(complexesFile));
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(fileConfiguration.getSeparator().charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (fileConfiguration.isHeader()) {
            csvWriter.writeNext(new String[]{ "complex_id", "participants" });
        }
        for (String complexId: complexesInIntact.keySet()) {
            Set<String> participantIds = complexesInIntact.get(complexId);
            csvWriter.writeNext(new String[]{ complexId, String.join(";", participantIds) });
        }
        csvWriter.close();

        log.info("Complexes saved");
        log.info("Saving all proteins");

        File proteinsFile = new File(reportDirectory, "proteins" + fileConfiguration.getExtension());
        writer = new BufferedWriter(new FileWriter(proteinsFile));
        csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(fileConfiguration.getSeparator().charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (fileConfiguration.isHeader()) {
            csvWriter.writeNext(new String[]{ "protein_id" });
        }
        for (String proteinId: proteinsInIntact) {
            csvWriter.writeNext(new String[]{ proteinId });
        }
        csvWriter.close();

        log.info("Proteins saved");
    }

    private void generateAndSaveUniprotMapping() throws IOException {
        log.info("Reading protein ids from file: " + fileConfiguration.getInputFileName());

        Map<String, String> uniprotProteinMapping;
        try {
            Collection<List<String>> proteinIds = proteinIdsReader.readProteinIdsFromFile();
            log.info("Read ids from file: " + proteinIds.size());
            log.info("Mapping to Uniprot");
            uniprotProteinMapping = mapToUniprotProteins(proteinIds);
            log.info("Map to Uniprot completed");
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }

        log.info("Saving all protein mappings");

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        File uniprotMappingFile = new File(reportDirectory, "uniprot_mapping" + fileConfiguration.getExtension());
        BufferedWriter writer = new BufferedWriter(new FileWriter(uniprotMappingFile));
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(fileConfiguration.getSeparator().charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (fileConfiguration.isHeader()) {
            csvWriter.writeNext(new String[]{ "old_protein_id", "new_protein_id" });
        }
        for (String oldProteinId: uniprotProteinMapping.keySet()) {
            String newProteinId = uniprotProteinMapping.get(oldProteinId);
            csvWriter.writeNext(new String[]{ oldProteinId, newProteinId });
        }
        csvWriter.close();

        log.info("Protein mappings saved");
    }

    private Map<String, String> mapToUniprotProteins(Collection<List<String>> proteinIds) throws IOException {
        Set<String> mainProteinIds = proteinIds.stream()
                .map(proteinIdList -> proteinIdList.stream()
                        .filter(proteinId -> !proteinId.matches("^(.+)(;\\1-\\d+)+$"))
                        .findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
        Map<String, List<UniprotProtein>> mapping = uniProtMappingService.mapIds(mainProteinIds);

        Map<String, String> mappingToReturn = new HashMap<>();
        for (String proteinId : mainProteinIds) {
            List<UniprotProtein> termMapping = mapping.get(proteinId);
            if (termMapping == null) {
                ignoredReportWriter.write(
                        proteinId,
                        String.format("%s has never existed", proteinId));
            } else if (termMapping.size() != 1) {
                if (termMapping.isEmpty()) {
                    ignoredReportWriter.write(
                            proteinId,
                            String.format("%s has been deleted", proteinId));
                } else {
                    ignoredReportWriter.write(
                            proteinId,
                            String.format(
                                    "%s has an ambiguous mapping to %s",
                                    proteinId,
                                    termMapping.stream().map(UniprotProtein::getProteinAc).collect(Collectors.joining(" and "))));
                }
            } else {
                String mappedId = termMapping.get(0).getProteinAc();
                if (!mappedId.equals(proteinId)) {
                    mappingToReturn.put(proteinId, mappedId);
                }
            }
        }

        return mappingToReturn;
    }

    private void initialiseOutputDirectory() throws IOException {
        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + fileConfiguration.getReportDirectory());
        }

        this.ignoredReportWriter = new ProteinFailedWriter(
                new File(reportDirectory, "ignored" + fileConfiguration.getExtension()),
                fileConfiguration.getSeparator(),
                fileConfiguration.isHeader());
    }
}
