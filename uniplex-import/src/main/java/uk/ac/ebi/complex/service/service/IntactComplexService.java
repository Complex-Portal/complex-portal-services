package uk.ac.ebi.complex.service.service;

import lombok.RequiredArgsConstructor;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import psidev.psi.mi.jami.model.Protein;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.ComplexFinder;
import uk.ac.ebi.complex.service.ComplexFinderResult;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.ComplexAcValue;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactOrganism;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;
import uk.ac.ebi.intact.jami.model.user.User;
import uk.ac.ebi.intact.jami.synchronizer.FinderException;
import uk.ac.ebi.intact.jami.synchronizer.PersisterException;
import uk.ac.ebi.intact.jami.synchronizer.SynchronizerException;

import javax.persistence.PersistenceException;
import java.util.Collection;

@Component
@RequiredArgsConstructor
public class IntactComplexService {

    private final ComplexFinder complexFinder;
    private final IntactDao intactDao;

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public ComplexFinderResult<IntactComplex> findComplexWithMatchingProteins(Collection<String> proteinAcs) {
        return this.complexFinder.findComplexWithMatchingProteins(proteinAcs);
    }

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public User geUser(String username) {
        return intactDao.getUserDao().getByLogin(username);
    }

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public IntactSource getSource(String id) {
        return intactDao.getSourceDao().getByMIIdentifier(id);
    }

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public IntactCvTerm getCvTerm(String clazz, String id) {
        return intactDao.getCvTermDao().getByUniqueIdentifier(id, clazz);
    }

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public Collection<IntactProtein> findProtein(String proteinId) {
        return intactDao.getProteinDao().getByXrefQualifier(Xref.IDENTITY, Xref.IDENTITY_MI, proteinId);
    }

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public IntactOrganism getOrganism(int taxId) {
        return intactDao.getOrganismDao().getByTaxidOnly(taxId);
    }

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public String getNextComplexAc() {
        return ComplexAcValue.getNextComplexAcValue(intactDao.getEntityManager());
    }

    @Retryable(
            include = PersistenceException.class,
            maxAttemptsExpression = "${retry.maxAttempts}",
            backoff = @Backoff(delayExpression = "${retry.maxDelay}", multiplierExpression = "${retry.multiplier}"))
    public IntactProtein convertProteinToPersistentObject(Protein protein) throws FinderException, SynchronizerException, PersisterException {
        return intactDao.getSynchronizerContext()
                .getProteinSynchronizer()
                .convertToPersistentObject(protein);
    }
}
