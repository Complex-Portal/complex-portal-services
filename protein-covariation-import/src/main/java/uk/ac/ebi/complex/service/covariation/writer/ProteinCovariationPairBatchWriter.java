package uk.ac.ebi.complex.service.covariation.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import uk.ac.ebi.complex.service.batch.writer.AbstractBatchWriter;
import uk.ac.ebi.complex.service.covariation.model.ProteinPairCovariation;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactProteinPairCovariation;

import javax.persistence.Query;
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

    @Override
    public void write(List<? extends List<ProteinPairCovariation>> items) throws Exception {
        List<IntactProteinPairCovariation> existingProteinPairCovariations = getProteinCovariations(items);

        List<IntactProteinPairCovariation> newCovariationsToSave = new ArrayList<>();
        List<IntactProteinPairCovariation> covariationsToUpdate = new ArrayList<>();

        for (List<ProteinPairCovariation> proteinCovariations : items) {
            for (ProteinPairCovariation proteinCovariation : proteinCovariations) {
                IntactProteinPairCovariation intactProteinPairCovariation = getProteinCovariationIfExists(
                        existingProteinPairCovariations, proteinCovariation);
                if (intactProteinPairCovariation == null) {
                    newCovariationsToSave.add(new IntactProteinPairCovariation(
                            proteinCovariation.getProteinA(), proteinCovariation.getProteinB(), proteinCovariation.getProbability()));
                } else if (!intactProteinPairCovariation.getProbability().equals(proteinCovariation.getProbability())) {
                    intactProteinPairCovariation.setProbability(proteinCovariation.getProbability());
                    covariationsToUpdate.add(intactProteinPairCovariation);
                }
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

    private List<IntactProteinPairCovariation> getProteinCovariations(List<? extends List<ProteinPairCovariation>> proteinCovariations) {
        Set<String> proteinIds = proteinCovariations.stream()
                .flatMap(Collection::stream)
                .flatMap(covariation -> Stream.of(covariation.getProteinA(), covariation.getProteinB()))
                .collect(Collectors.toSet());

        Query query = intactDao.getEntityManager().createQuery(
                "select c " +
                        "from IntactProteinPairCovariation c " +
                        "where c.proteinA in (:proteins) or c.proteinB in (:proteins)");
        query.setParameter("proteins", proteinIds);

        return query.getResultList();
    }

    private IntactProteinPairCovariation getProteinCovariationIfExists(
            List<IntactProteinPairCovariation> intactProteinCovariations,
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
}
