package uk.ac.ebi.complex.service.pdb.reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.ModelledComparableParticipant;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.CvTermUtils;
import psidev.psi.mi.jami.utils.XrefUtils;
import psidev.psi.mi.jami.utils.comparator.CollectionComparator;
import psidev.psi.mi.jami.utils.comparator.participant.ModelledComparableParticipantComparator;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.manager.ComplexManager;
import uk.ac.ebi.complex.service.pdb.model.AssemblyEntry;
import uk.ac.ebi.complex.service.pdb.model.ComplexWithAssemblies;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;
import uk.ac.ebi.intact.jami.service.ComplexService;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@Component
@RequiredArgsConstructor
public class PdbAssembliesReader implements ItemReader<ComplexWithAssemblies>, ItemStream {

    private final IntactDao intactDao;
    private final ComplexService complexService;
    private final FileConfiguration fileConfiguration;
    private final PdbAssembliesFileReader pdbAssembliesFileReader;

    private CollectionComparator<ModelledComparableParticipant> comparableParticipantsComparator;
    private Iterator<Complex> complexIterator;
    private Map<String, IntactProtein> proteinCacheMap;
    private Set<AssemblyEntry> assemblies;

    @Override
    public ComplexWithAssemblies read() {
        while (complexIterator.hasNext()) {
            Complex complex = complexIterator.next();
            IntactComplex intactComplex = (IntactComplex) intactDao.getEntityManager().merge(complex);
            String complexAc = intactComplex.getComplexAc();

            boolean complexReleasedOrReadyForRelease = LifeCycleStatus.RELEASED.equals(intactComplex.getStatus()) ||
                    LifeCycleStatus.READY_FOR_RELEASE.equals(intactComplex.getStatus());

            if (complexReleasedOrReadyForRelease) {

                Collection<ModelledComparableParticipant> complexProteins = getProteinComponents(intactComplex, proteinCacheMap);

                Set<String> assembliesForComplex = new HashSet<>();
                for (AssemblyEntry assemblyEntry : assemblies) {
                    for (String complexId : assemblyEntry.getComplexIds()) {
                        if (complexId.equals(complexAc)) {
                            assembliesForComplex.addAll(assemblyEntry.getAssemblies());
                        }
                    }

                    Collection<ModelledComparableParticipant> assemblyProteins = assemblyEntry.getProteins().stream()
                            .map(protein -> new ModelledComparableParticipant(
                                    protein.getProteinAc(),
                                    List.of(),
                                    1,
                                    CvTermUtils.createProteinInteractorType()))
                            .collect(Collectors.toList());

                    if (this.comparableParticipantsComparator.compare(complexProteins, assemblyProteins) == 0) {
                        assembliesForComplex.addAll(assemblyEntry.getAssemblies());
                    }
                }

                if (!assembliesForComplex.isEmpty()) {
                    return new ComplexWithAssemblies(complexAc, intactComplex.isPredictedComplex(), assembliesForComplex);
                }

                Collection<Xref> pdbIdentifiers = XrefUtils.collectAllXrefsHavingDatabase(
                        intactComplex.getIdentifiers(), ComplexManager.WWPDB_DB_MI, ComplexManager.WWPDB_DB_NAME);
                Collection<Xref> pdbXrefs = XrefUtils.collectAllXrefsHavingDatabase(
                        intactComplex.getXrefs(), ComplexManager.WWPDB_DB_MI, ComplexManager.WWPDB_DB_NAME);
                if (!pdbIdentifiers.isEmpty() || !pdbXrefs.isEmpty()) {
                    return new ComplexWithAssemblies(complexAc, intactComplex.isPredictedComplex(), new HashSet<>());
                }
            }
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        try {
            ModelledComparableParticipantComparator participantComparator = new ModelledComparableParticipantComparator();
            // Ignore stoichiometry for now
            participantComparator.setIgnoreStoichiometry(true);
            this.comparableParticipantsComparator = new CollectionComparator<>(participantComparator);

            proteinCacheMap = new HashMap<>();

            File parsedFile = fileConfiguration.outputPath().toFile();
            assemblies = pdbAssembliesFileReader.readAssembliesFromParsedFile(parsedFile);

            this.complexIterator = complexService.iterateAll();
        } catch (IOException e) {
            throw new ItemStreamException("Input file could not be read: " + fileConfiguration.outputPath(), e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {
    }

    private Collection<ModelledComparableParticipant> getProteinComponents(
            IntactComplex complex,
            Map<String, IntactProtein> proteinCacheMap) {

        return complex.getComparableParticipants(
                true,
                proteinAc -> {
                    if (!proteinCacheMap.containsKey(proteinAc)) {
                        IntactProtein protein = intactDao.getProteinDao().getByAc(proteinAc);
                        proteinCacheMap.put(proteinAc, protein);
                    }
                    return proteinCacheMap.get(proteinAc);
                }
        );
    }
}
