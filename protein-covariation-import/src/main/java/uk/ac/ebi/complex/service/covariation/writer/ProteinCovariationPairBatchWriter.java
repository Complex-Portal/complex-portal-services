package uk.ac.ebi.complex.service.covariation.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.CvTerm;
import uk.ac.ebi.complex.service.batch.exception.SourceNotFoundException;
import uk.ac.ebi.complex.service.batch.writer.AbstractBatchWriter;
import uk.ac.ebi.complex.service.covariation.model.ProteinPairCovariation;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactProteinPairCovariation;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j
@SuperBuilder
public class ProteinCovariationPairBatchWriter extends AbstractBatchWriter<List<ProteinPairCovariation>, IntactProteinPairCovariation> {

    private final IntactDao intactDao;
    private final String databaseId;

    private CvTerm database;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);

        try {
            database = findDatabase();
        } catch (SourceNotFoundException e) {
            throw new ItemStreamException(e);
        }
    }

    @Override
    public void write(List<? extends List<ProteinPairCovariation>> items) throws Exception {
        Collection<ProteinPairCovariation> covariationsWithoutDuplicates = mergeDuplicates(items);
        Collection<IntactProteinPairCovariation> existingProteinPairCovariationsInDb = getProteinCovariations(items);
        List<IntactProteinPairCovariation> newCovariationsToSave = new ArrayList<>();
        List<IntactProteinPairCovariation> covariationsToUpdate = new ArrayList<>();

        for (ProteinPairCovariation proteinCovariation : covariationsWithoutDuplicates) {
            IntactProteinPairCovariation intactProteinPairCovariation = getProteinCovariationIfExists(
                    existingProteinPairCovariationsInDb, proteinCovariation);
            if (intactProteinPairCovariation == null) {
                newCovariationsToSave.add(new IntactProteinPairCovariation(
                        proteinCovariation.getProteinA(),
                        proteinCovariation.getProteinB(),
                        proteinCovariation.getProbability(),
                        database));
            } else if (proteinCovariation.getProbability() > intactProteinPairCovariation.getProbability()) {
                intactProteinPairCovariation.setProbability(proteinCovariation.getProbability());
                covariationsToUpdate.add(intactProteinPairCovariation);
            }
        }

        if (!covariationsToUpdate.isEmpty()) {
            this.intactService.saveOrUpdate(covariationsToUpdate);
        }
        if (!newCovariationsToSave.isEmpty()) {
            newCovariationsToSave.forEach(intactProteinPairCovariation ->
                    this.intactDao.getEntityManager().persist(intactProteinPairCovariation));
            this.intactDao.getEntityManager().flush();
        }
    }

    private Collection<IntactProteinPairCovariation> getProteinCovariations(List<? extends List<ProteinPairCovariation>> proteinCovariations) {
        Set<String> proteinIds = proteinCovariations.stream()
                .flatMap(Collection::stream)
                .flatMap(covariation -> Stream.of(covariation.getProteinA(), covariation.getProteinB()))
                .collect(Collectors.toSet());

        return intactDao.getIntactProteinPairCovariationDao().getAllForProteinIdsAndDatabase(proteinIds, databaseId);
    }

    private IntactProteinPairCovariation getProteinCovariationIfExists(
            Collection<IntactProteinPairCovariation> intactProteinCovariations,
            ProteinPairCovariation proteinCovariation) {

        for (IntactProteinPairCovariation intactProteinPairCovariation : intactProteinCovariations) {
            if (intactProteinPairCovariation.getProteinA().equals(proteinCovariation.getProteinA()) &&
                    intactProteinPairCovariation.getProteinB().equals(proteinCovariation.getProteinB())) {
                return intactProteinPairCovariation;
            }
            if (intactProteinPairCovariation.getProteinA().equals(proteinCovariation.getProteinB()) &&
                    intactProteinPairCovariation.getProteinB().equals(proteinCovariation.getProteinA())) {
                return intactProteinPairCovariation;
            }
        }

        return null;
    }

    private Collection<ProteinPairCovariation> mergeDuplicates(List<? extends List<ProteinPairCovariation>> covariations) {
        return covariations.stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(
                        covariation -> (covariation.getProteinA().compareTo(covariation.getProteinB()) < 0)
                                ? covariation.getProteinA() + "_" + covariation.getProteinB()
                                : covariation.getProteinB() + "_" + covariation.getProteinA(),
                        covariation -> covariation,
                        (covariationA, covariationB) -> {
                            if (covariationA.getProbability() > covariationB.getProbability()) {
                                return covariationA;
                            } else {
                                return covariationB;
                            }
                        }
                )).values();
    }

    private IntactCvTerm findDatabase() throws SourceNotFoundException {
        IntactCvTerm databaseTerm = intactDao.getCvTermDao().getByMIIdentifier(databaseId, IntactUtils.DATABASE_OBJCLASS);
        if (databaseTerm != null) {
            return databaseTerm;
        }
        throw new SourceNotFoundException("Database not found with id '" + databaseId + "'");
    }
}
