package uk.ac.ebi.complex.service.processor;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.logging.ProteinFailedWriter;
import uk.ac.ebi.complex.service.model.ProteinCovariation;
import uk.ac.ebi.complex.service.model.ProteinPairCovariation;
import uk.ac.ebi.complex.service.model.UniprotProtein;
import uk.ac.ebi.complex.service.reader.ProteinIdsReader;
import uk.ac.ebi.complex.service.service.UniProtMappingService;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@SuperBuilder
public class ProteinCovariationBatchProcessor extends AbstractBatchProcessor<List<ProteinCovariation>, List<ProteinPairCovariation>> {

    private final IntactDao intactDao;
    private final UniProtMappingService uniProtMappingService;
    private final ProteinIdsReader proteinIdsReader;

    private ProteinFailedWriter ignoredReportWriter;

    private Map<String, String> uniprotProteinMapping;

    private Set<String> proteinIdsNotInIntact;
    private Set<String> proteinIdsInIntact;

    @Override
    public List<ProteinPairCovariation> process(List<ProteinCovariation> items) throws Exception {
        List<ProteinPairCovariation> allProteinCovariationsPairs = new ArrayList<>();
        for (ProteinCovariation proteinCovariation : items) {
            allProteinCovariationsPairs.addAll(expandProteinCovariation(proteinCovariation));
        }
        if (!allProteinCovariationsPairs.isEmpty()) {
            return allProteinCovariationsPairs;
        }
        return null;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.ignoredReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);

        proteinIdsNotInIntact = new HashSet<>();
        proteinIdsInIntact = new HashSet<>();

        System.out.println("Reading ids from file: " + fileConfiguration.getInputFileName());

        try {
            Collection<List<String>> proteinIds = proteinIdsReader.readProteinIdsFromFile();
            System.out.println("Read ids from file: " + proteinIds.size());
            System.out.println("Mapping to Uniprot");
            uniprotProteinMapping = mapToUniprotProteins(proteinIds);
            System.out.println("Map to Uniprot completed");
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.ignoredReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());

        this.ignoredReportWriter = new ProteinFailedWriter(
                new File(reportDirectory, "ignored" + fileConfiguration.getExtension()),
                fileConfiguration.getSeparator(),
                fileConfiguration.isHeader());
    }

    protected List<ProteinPairCovariation> expandProteinCovariation(ProteinCovariation proteinCovariation) {
        Set<String> allProteinIds = new HashSet<>();
        allProteinIds.addAll(proteinCovariation.getProteinA());
        allProteinIds.addAll(proteinCovariation.getProteinB());

        Set<String> newProteinIdsToSearch = allProteinIds.stream()
                .filter(id -> !proteinIdsNotInIntact.contains(id))
                .filter(id -> !proteinIdsInIntact.contains(id))
                .collect(Collectors.toSet());

        if (!newProteinIdsToSearch.isEmpty()) {
            Collection<IntactProtein> proteins = this.intactDao.getProteinDao().getByCanonicalIds(Xref.UNIPROTKB_MI, newProteinIdsToSearch);
            Set<String> proteinIdsInDatabase = proteins.stream()
                    .map(IntactProtein::getPreferredIdentifier)
                    .map(Xref::getId)
                    .collect(Collectors.toSet());

            proteinIdsInIntact.addAll(proteinIdsInDatabase);
            proteinIdsNotInIntact.addAll(newProteinIdsToSearch.stream()
                    .filter(id -> !proteinIdsInDatabase.contains(id))
                    .collect(Collectors.toSet()));
        }

        List<ProteinPairCovariation> pairs = new ArrayList<>();

        for (String proteinA : proteinCovariation.getProteinA()) {
            if (proteinIdsInIntact.contains(proteinA)) {
                for (String proteinB : proteinCovariation.getProteinB()) {
                    if (proteinIdsInIntact.contains(proteinB)) {
                        pairs.add(new ProteinPairCovariation(proteinA, proteinB, proteinCovariation.getProbability()));
                        if (uniprotProteinMapping.containsKey(proteinA)) {
                            pairs.add(new ProteinPairCovariation(uniprotProteinMapping.get(proteinA), proteinB, proteinCovariation.getProbability()));
                            if (uniprotProteinMapping.containsKey(proteinB)) {
                                pairs.add(new ProteinPairCovariation(uniprotProteinMapping.get(proteinA), uniprotProteinMapping.get(proteinB), proteinCovariation.getProbability()));
                            }
                        }
                        if (uniprotProteinMapping.containsKey(proteinB)) {
                            pairs.add(new ProteinPairCovariation(proteinA, uniprotProteinMapping.get(proteinB), proteinCovariation.getProbability()));
                        }
                    }
                }
            }
        }

        return pairs;
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
}
