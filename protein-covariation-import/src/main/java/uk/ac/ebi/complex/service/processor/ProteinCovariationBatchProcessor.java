package uk.ac.ebi.complex.service.processor;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.Interactor;
import psidev.psi.mi.jami.model.ModelledComparableParticipant;
import psidev.psi.mi.jami.model.ModelledParticipant;
import psidev.psi.mi.jami.model.Participant;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.logging.ProteinFailedWriter;
import uk.ac.ebi.complex.service.model.ProteinCovariation;
import uk.ac.ebi.complex.service.model.ProteinPairCovariation;
import uk.ac.ebi.complex.service.model.UniprotProtein;
import uk.ac.ebi.complex.service.reader.ProteinIdsReader;
import uk.ac.ebi.complex.service.service.UniProtMappingService;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.service.ComplexService;
import uk.ac.ebi.intact.jami.service.InteractorService;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
@SuperBuilder
public class ProteinCovariationBatchProcessor extends AbstractBatchProcessor<List<ProteinCovariation>, List<ProteinPairCovariation>> {

    private final IntactDao intactDao;
    private final UniProtMappingService uniProtMappingService;
    private final ProteinIdsReader proteinIdsReader;
    private final ComplexService complexService;

    private ProteinFailedWriter ignoredReportWriter;

    private Map<String, String> uniprotProteinMapping;

//    private Set<String> proteinIdsNotInIntact;
//    private Map<String, String> proteinIdsInIntact;
    
    private Set<String> proteinInIntact;
    private Map<String, Set<String>> complexesInIntact;

    @Override
    public List<ProteinPairCovariation> process(List<ProteinCovariation> items) throws Exception {
        List<ProteinPairCovariation> allProteinCovariationsPairs = expandProteinCovariationsV2(items);
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

//        proteinIdsNotInIntact = new HashSet<>();
//        proteinIdsInIntact = new HashMap<>();

        System.out.println("Reading all complexes and proteins");

        long count = 0;
        complexesInIntact = new HashMap<>();
        proteinInIntact = new HashSet<>();

        Iterator<Complex> complexIterator = complexService.iterateAll(true);
        while (complexIterator.hasNext()) {
            count++;
            if (count % 500 == 0) {
                System.out.println("Processed " + count + " complexes");
            }
            Complex complex = complexIterator.next();
            Set<String> participantIds = new HashSet<>();
//            IntactComplex intactComplex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(complex.getComplexAc());
//            for (ModelledComparableParticipant participant: complex.getComparableParticipants()) {
//                participantIds.add(participant.getInteractorId());
//            }
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
            complexesInIntact.put(complex.getPreferredIdentifier().getId(), participantIds);
            proteinInIntact.addAll(participantIds);
        }

        System.out.println("Reading all complexes and proteins - DONE");

        System.out.println("Reading protein ids from file: " + fileConfiguration.getInputFileName());

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

    protected List<ProteinPairCovariation> expandProteinCovariationsV2(List<ProteinCovariation> proteinCovariations) {
        List<ProteinPairCovariation> pairs = new ArrayList<>();

        for (ProteinCovariation proteinCovariation : proteinCovariations) {
            for (String proteinA : proteinCovariation.getProteinA()) {
                for (String proteinB : proteinCovariation.getProteinB()) {
                    addCovariationIfProteinsPartOfComplexV2(pairs, proteinA, proteinB, proteinCovariation.getProbability());
                }
            }
        }

        return pairs;
    }

    private void addCovariationIfProteinsPartOfComplexV2(
            List<ProteinPairCovariation> pairs,
            String proteinA,
            String proteinB,
            Double probability) {

        if (isProteinPairPartOfComplexV2(proteinA, proteinB)) {
            pairs.add(new ProteinPairCovariation(proteinA, proteinB, probability));
        }

        if (uniprotProteinMapping.containsKey(proteinA)) {
            String newProteinA = uniprotProteinMapping.get(proteinA);
            if (isProteinPairPartOfComplexV2(newProteinA, proteinB)) {
                pairs.add(new ProteinPairCovariation(newProteinA, proteinB, probability));
            }

            if (uniprotProteinMapping.containsKey(proteinB)) {
                String newProteinB = uniprotProteinMapping.get(proteinB);
                if (isProteinPairPartOfComplexV2(newProteinA, newProteinB)) {
                    pairs.add(new ProteinPairCovariation(newProteinA, newProteinB, probability));
                }
            }
        }
        if (uniprotProteinMapping.containsKey(proteinB)) {
            String newProteinB = uniprotProteinMapping.get(proteinB);
            if (isProteinPairPartOfComplexV2(proteinA, newProteinB)) {
                pairs.add(new ProteinPairCovariation(proteinA, newProteinB, probability));
            }
        }
    }

    private boolean isProteinPairPartOfComplexV2(String proteinA, String proteinB) {
        if (proteinInIntact.contains(proteinA)) {
            if (proteinInIntact.contains(proteinB)) {
                for (String complexId : complexesInIntact.keySet()) {
                    Set<String> participants = complexesInIntact.get(complexId);
                    if (participants.stream().anyMatch(p -> p.equals(proteinA))) {
                        if (participants.stream().anyMatch(p -> p.equals(proteinB))) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

//    protected List<ProteinPairCovariation> expandProteinCovariations(List<ProteinCovariation> proteinCovariations) {
//        Set<String> allProteinIds = new HashSet<>();
//        for (ProteinCovariation proteinCovariation : proteinCovariations) {
//            allProteinIds.addAll(proteinCovariation.getProteinA());
//            allProteinIds.addAll(proteinCovariation.getProteinB());
//        }
//
//        Set<String> newProteinIdsToSearch = allProteinIds.stream()
//                .filter(id -> !proteinIdsNotInIntact.contains(id))
//                .filter(id -> !proteinIdsInIntact.containsKey(id))
//                .collect(Collectors.toSet());
//
//        if (!newProteinIdsToSearch.isEmpty()) {
//            Collection<IntactProtein> proteins = this.intactDao.getProteinDao().getByCanonicalIds(Xref.UNIPROTKB_MI, newProteinIdsToSearch);
//            proteins.forEach(protein ->
//                    proteinIdsInIntact.putIfAbsent(protein.getPreferredIdentifier().getId(), protein.getAc()));
//
//            proteinIdsNotInIntact.addAll(newProteinIdsToSearch.stream()
//                    .filter(id -> !proteinIdsInIntact.containsKey(id))
//                    .collect(Collectors.toSet()));
//        }
//
//        Set<String> proteinIntactAcs = allProteinIds.stream()
//                .filter(proteinIdsInIntact::containsKey)
//                .map(proteinIdsInIntact::get)
//                .collect(Collectors.toSet());
//        Collection<IntactComplex> complexes = getComplexesWithProteins(proteinIntactAcs);
//
//        List<ProteinPairCovariation> pairs = new ArrayList<>();
//
//        for (ProteinCovariation proteinCovariation : proteinCovariations) {
//            for (String proteinA : proteinCovariation.getProteinA()) {
//                for (String proteinB : proteinCovariation.getProteinB()) {
//                    addCovariationIfProteinsPartOfComplex(complexes, pairs, proteinA, proteinB, proteinCovariation.getProbability());
//                }
//            }
//        }
//
//        return pairs;
//    }
//
//    private void addCovariationIfProteinsPartOfComplex(
//            Collection<IntactComplex> complexes,
//            List<ProteinPairCovariation> pairs,
//            String proteinA,
//            String proteinB,
//            Double probability) {
//
//        if (isProteinPairPartOfComplex(complexes, proteinA, proteinB)) {
//            pairs.add(new ProteinPairCovariation(proteinA, proteinB, probability));
//        }
//
//        if (uniprotProteinMapping.containsKey(proteinA)) {
//            String newProteinA = uniprotProteinMapping.get(proteinA);
//            if (isProteinPairPartOfComplex(complexes, newProteinA, proteinB)) {
//                pairs.add(new ProteinPairCovariation(newProteinA, proteinB, probability));
//            }
//
//            if (uniprotProteinMapping.containsKey(proteinB)) {
//                String newProteinB = uniprotProteinMapping.get(proteinB);
//                if (isProteinPairPartOfComplex(complexes, newProteinA, newProteinB)) {
//                    pairs.add(new ProteinPairCovariation(newProteinA, newProteinB, probability));
//                }
//            }
//        }
//        if (uniprotProteinMapping.containsKey(proteinB)) {
//            String newProteinB = uniprotProteinMapping.get(proteinB);
//            if (isProteinPairPartOfComplex(complexes, proteinA, newProteinB)) {
//                pairs.add(new ProteinPairCovariation(proteinA, newProteinB, probability));
//            }
//        }
//    }
//
//    private Collection<IntactComplex> getComplexesWithProteins(Set<String> proteins) {
//        return this.intactDao.getComplexDao()
//                .getComplexesInvolvingProteinsWithEbiAcs(proteins);
//    }
//
//    private boolean isProteinPairPartOfComplex(Collection<IntactComplex> complexes, String proteinA, String proteinB) {
//        if (proteinIdsInIntact.containsKey(proteinA)) {
//            if (proteinIdsInIntact.containsKey(proteinB)) {
//                for (IntactComplex intactComplex : complexes) {
//                    Collection<ModelledParticipant> participants  = intactComplex.getParticipants();
//                    if (participants.stream().anyMatch(p -> p.getInteractor().getPreferredIdentifier().getId().equals(proteinA))) {
//                        if (participants.stream().anyMatch(p -> p.getInteractor().getPreferredIdentifier().getId().equals(proteinB))) {
//                            return true;
//                        }
//                    }
//                }
//            }
//        }
//        return false;
//    }

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
