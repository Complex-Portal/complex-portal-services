package uk.ac.ebi.complex.service.covariation.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.Source;
import uk.ac.ebi.complex.service.batch.exception.SourceNotFoundException;
import uk.ac.ebi.complex.service.batch.writer.AbstractBatchWriter;
import uk.ac.ebi.complex.service.covariation.model.ProteinPairCovariation;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactProteinPairCovariation;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;

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
    private final String sourceId;

    private Source source;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);

        try {
            source = findSource();
        } catch (SourceNotFoundException e) {
            throw new ItemStreamException(e);
        }
    }

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
                            proteinCovariation.getProteinA(),
                            proteinCovariation.getProteinB(),
                            proteinCovariation.getProbability(),
                            source));
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
                        "where (c.proteinA in (:proteins) or c.proteinB in (:proteins)) " +
                        "and c.source = :sourceName");
        query.setParameter("proteins", proteinIds);
        query.setParameter("sourceName", source.getShortName());

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

    private IntactSource findSource() throws SourceNotFoundException {
        IntactSource source = intactDao.getSourceDao().getByMIIdentifier(sourceId);
        if (source != null) {
            return source;
        }
        Collection<IntactSource> sourcesByXref = intactDao.getSourceDao().getByXref(sourceId);
        if (sourcesByXref != null && !sourcesByXref.isEmpty()) {
            List<IntactSource> sourcesWithIdentifier = sourcesByXref.stream()
                    .filter(sourceByXref -> sourceByXref.getIdentifiers().stream().anyMatch(xrefId -> sourceId.equals(xrefId.getId())))
                    .collect(Collectors.toList());
            if (sourcesWithIdentifier.size() == 1) {
                return sourcesWithIdentifier.get(0);
            }
        }
        throw new SourceNotFoundException("Source not found with id '" + sourceId + "'");
    }
}
