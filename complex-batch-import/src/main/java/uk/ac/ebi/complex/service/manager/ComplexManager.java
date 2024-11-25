package uk.ac.ebi.complex.service.manager;

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import psidev.psi.mi.jami.bridges.exception.BridgeFailedException;
import psidev.psi.mi.jami.bridges.uniprot.UniprotProteinFetcher;
import psidev.psi.mi.jami.model.Alias;
import psidev.psi.mi.jami.model.Annotation;
import psidev.psi.mi.jami.model.Entity;
import psidev.psi.mi.jami.model.Protein;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.config.AppProperties;
import uk.ac.ebi.complex.service.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.exception.OrganismNotFoundException;
import uk.ac.ebi.complex.service.exception.ProteinException;
import uk.ac.ebi.complex.service.exception.SourceNotFoundException;
import uk.ac.ebi.complex.service.exception.UserNotFoundException;
import uk.ac.ebi.complex.service.model.ComplexToImport;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.ComplexAcValue;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactModelledParticipant;
import uk.ac.ebi.intact.jami.model.extension.IntactOrganism;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;
import uk.ac.ebi.intact.jami.model.extension.IntactStoichiometry;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;
import uk.ac.ebi.intact.jami.model.lifecycle.ComplexLifeCycleEvent;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleEventType;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;
import uk.ac.ebi.intact.jami.model.user.User;
import uk.ac.ebi.intact.jami.synchronizer.FinderException;
import uk.ac.ebi.intact.jami.synchronizer.PersisterException;
import uk.ac.ebi.intact.jami.synchronizer.SynchronizerException;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Log4j
@RequiredArgsConstructor
@SuperBuilder
public abstract class ComplexManager<T, R extends ComplexToImport<T>> {

    private static final String STABLE_COMPLEX_MI = "MI:1302";
    private static final String PHYSICAL_ASSOCIATION_MI = "MI:0915";
    private static final String GENE_NAME_MI = "MI:0301";
    private static final Integer HUMAN_TAX_ID = 9606;
    private static final String READY_FOR_RELEASE_COMPLEX_PUBMED_ID = "14681455";

    public static final String AUTHOR_CONFIDENCE_TOPIC_ID = "MI:0621";
    public static final String SUBSET_QUALIFIER = "subset";
    public static final String SUBSET_QUALIFIER_MI = "MI:2179";
    public static final String COMPLEX_CLUSTER_QUALIFIER = "complex-cluster";
    public static final String COMPLEX_CLUSTER_QUALIFIER_MI = "MI:2427";

    private final IntactDao intactDao;
    private final UniprotProteinFetcher uniprotProteinFetcher;
    private final AppProperties appProperties;

    private final Map<String, IntactCvTerm> cvTermMap = new HashMap<>();
    private final Map<String, IntactSource> sourceMap = new HashMap<>();
    private final Map<String, IntactProtein> proteinMap = new HashMap<>();
    private final Map<String, User> userMap = new HashMap<>();
    private final Map<Integer, IntactOrganism> organismMap = new HashMap<>();

    public abstract boolean doesComplexHasIdentityXref(R newComplex, IntactComplex existingComplex);

    public abstract boolean doesComplexNeedUpdating(R newComplex, IntactComplex existingComplex);

    public abstract boolean doesComplexNeedSubsetXref(R newComplex, IntactComplex existingComplex);

    public abstract boolean doesComplexNeedComplexClusterXref(R newComplex, IntactComplex existingComplex);

    public abstract IntactComplex addSubsetXrefs(R newComplex, IntactComplex existingComplex) throws CvTermNotFoundException;

    public abstract IntactComplex addComplexClusterXrefs(R newComplex, IntactComplex existingComplex) throws CvTermNotFoundException;

    public IntactComplex mergeComplexWithExistingComplex(R newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addIdentityXrefs(newComplex, existingComplex);
        addConfidenceAnnotation(newComplex, existingComplex);
        return existingComplex;
    }

    public IntactComplex newComplex(R newComplex)
            throws BridgeFailedException, FinderException, SynchronizerException, PersisterException,
            SourceNotFoundException, CvTermNotFoundException, ProteinException, UserNotFoundException,
            OrganismNotFoundException {

        IntactComplex complex = new IntactComplex(newComplex.getComplexIds().iterator().next());
        setComplexAc(complex);
        setOrganism(complex);
        setComplexSource(complex);
        setComplexType(complex);
        setComplexEvidenceType(complex);
        addIdentityXrefs(newComplex, complex);
        addConfidenceAnnotation(newComplex, complex);
        setComplexComponents(newComplex, complex);
        setComplexSystematicName(complex);
        setComplexStatus(complex);
        setExperimentAndPublication(complex);
        complex.setPredictedComplex(true);
        complex.setCreatedDate(new Date());
        complex.setUpdatedDate(complex.getCreatedDate());

        return complex;
    }

    protected boolean doesComplexNeedXref(R newComplex, IntactComplex existingComplex, String databaseMi, String qualifierMi) {
        for (String clusterId: newComplex.getComplexIds()) {
            // If any of the cluster ids is missing, then the complex needs updating
            if (doesComplexNeedXref(clusterId, existingComplex, databaseMi, qualifierMi)) {
                return true;
            }
        }
        return false;
    }

    protected boolean doesComplexNeedXref(String xrefId, IntactComplex existingComplex, String databaseMi, String qualifierMi) {
        Collection<Xref> existingXrefs = getXrefs(existingComplex, databaseMi, qualifierMi);
        // If any of the cluster ids is missing, then the complex needs updating
        return existingXrefs.stream().noneMatch(id -> id.getId().equals(xrefId));
    }

    protected void addXrefs(R newComplex, IntactComplex existingComplex, String databaseMi, String qualifierMi, boolean isIdentifier) throws CvTermNotFoundException {
        for (String clusterId: newComplex.getComplexIds()) {
            addXrefs(clusterId, existingComplex, databaseMi, qualifierMi, isIdentifier);
        }
    }

    protected void addXrefs(String xrefId, IntactComplex existingComplex, String databaseMi, String qualifierMi, boolean isIdentifier) throws CvTermNotFoundException {
        Collection<Xref> existingXrefs = getXrefs(existingComplex, databaseMi, qualifierMi);
        if (existingXrefs.stream().noneMatch(id -> id.getId().equals(xrefId))) {
            InteractorXref xref = newXref(xrefId, databaseMi, qualifierMi);
            // We add the new xrefs to identifiers as we are using the identity qualifier.
            // If we eventually use another qualifier, we should add them to xrefs.
            if (isIdentifier) {
                existingComplex.getIdentifiers().add(xref);
            } else {
                existingComplex.getXrefs().add(xref);
            }
        }
    }

    public Collection<Xref> getXrefs(IntactComplex existingComplex, String databaseMi, String qualifierMi) {
        return ImmutableList.<Xref>builder()
                .addAll(getXrefs(existingComplex.getIdentifiers(), databaseMi, qualifierMi))
                .addAll(getXrefs(existingComplex.getXrefs(), databaseMi, qualifierMi))
                .build();
    }

    private Collection<Xref> getXrefs(Collection<Xref> xrefs, String databaseMi, String qualifierMi) {
        return xrefs
                .stream()
                .filter(id -> id.getDatabase() != null && databaseMi.equals(id.getDatabase().getMIIdentifier()))
                .filter(id -> id.getQualifier() != null && qualifierMi.equals(id.getQualifier().getMIIdentifier()))
                .collect(Collectors.toList());
    }

    protected InteractorXref newXref(String id, String databaseMi, String qualifierMi) throws CvTermNotFoundException {
        IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, databaseMi);
        // Currently we use identity as qualifier, as we are only importing exact matches.
        // If we merge curated complexes with partial matches, we need to add a different qualifier (subset, see-also, etc.).
        IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, qualifierMi);
        return new InteractorXref(database, id, qualifier);
    }

    protected Collection<Annotation> getAuthorConfidenceAnnotations(IntactComplex existingComplex) {
        return existingComplex.getAnnotations()
                .stream()
                .filter(ann -> AUTHOR_CONFIDENCE_TOPIC_ID.equals(ann.getTopic().getMIIdentifier()))
                .collect(Collectors.toList());
    }

    protected abstract void setComplexEvidenceType(IntactComplex complex) throws CvTermNotFoundException;

    protected abstract void addIdentityXrefs(R newComplex, IntactComplex existingComplex) throws CvTermNotFoundException;

    protected abstract void addConfidenceAnnotation(R newComplex, IntactComplex exsitingComplex) throws CvTermNotFoundException;

    protected abstract void setComplexSource(IntactComplex complex) throws SourceNotFoundException;

    private void setExperimentAndPublication(IntactComplex complex) {
        IntactUtils.createAndAddDefaultExperimentForComplexes(complex, READY_FOR_RELEASE_COMPLEX_PUBMED_ID);
    }

    private void setOrganism(IntactComplex complex) throws OrganismNotFoundException {
        // Currently we are only importing human complexes.
        // If we want to add support for other organisms, we need to update this.
        complex.setOrganism(findOrganism(HUMAN_TAX_ID));
    }

    private void setComplexStatus(IntactComplex complex) throws UserNotFoundException {
        User user = findUser(appProperties.getUserContextId());
        complex.setCreator(user.getLogin());
        complex.setUpdator(user.getLogin());

        complex.getLifecycleEvents().add(
                new ComplexLifeCycleEvent(LifeCycleEventType.CREATED, user, "New predicted complex"));
        complex.getLifecycleEvents().add(
                new ComplexLifeCycleEvent(LifeCycleEventType.READY_FOR_RELEASE, user, "Predicted complex ready for release"));
        complex.setStatus(LifeCycleStatus.READY_FOR_RELEASE);
    }

    private IntactProtein getIntactProtein(String proteinId) throws BridgeFailedException, FinderException, SynchronizerException, PersisterException, ProteinException {
        if (proteinMap.containsKey(proteinId)) {
            return proteinMap.get(proteinId);
        }
        Collection<IntactProtein> proteinByXref = intactDao.getProteinDao().getByXrefQualifier(Xref.IDENTITY, Xref.IDENTITY_MI, proteinId);
        if (!proteinByXref.isEmpty()) {
            if (proteinByXref.size() == 1) {
                return proteinByXref.iterator().next();
            }
            throw new ProteinException("Multiple proteins found in the DB for protein id '" + proteinId + "'");
        } else {
            Collection<Protein> uniprotProteins = uniprotProteinFetcher.fetchByIdentifier(proteinId);
            if (!uniprotProteins.isEmpty()) {
                if (uniprotProteins.size() == 1) {
                    IntactProtein intactProtein = intactDao.getSynchronizerContext()
                            .getProteinSynchronizer()
                            .convertToPersistentObject(uniprotProteins.iterator().next());
                    proteinMap.put(proteinId, intactProtein);
                    return intactProtein;
                }
                throw new ProteinException("Multiple proteins fetch from UniProt for protein id '" + proteinId + "'");
            }
            throw new ProteinException("No proteins fetch from UniProt for protein id '" + proteinId + "'");
        }
    }

    private void setComplexComponents(R newComplex, IntactComplex complex) throws BridgeFailedException, FinderException, SynchronizerException, PersisterException, ProteinException {
        for (String uniprotAc: newComplex.getProteinIds()) {
            IntactProtein intactProtein = getIntactProtein(uniprotAc);
            complex.getParticipants().add(new IntactModelledParticipant(intactProtein, new IntactStoichiometry(0)));
        }
    }

    private void setComplexAc(IntactComplex complex) throws CvTermNotFoundException {
        IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, Xref.COMPLEX_PORTAL_MI);
        IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.COMPLEX_PRIMARY_MI);
        // In future versions we may need to increase the version
        String version = "1";
        String acValue = ComplexAcValue.getNextComplexAcValue(intactDao.getEntityManager());
        InteractorXref xref = new InteractorXref(database, acValue, version, qualifier);
        complex.getIdentifiers().add(xref);
    }

    protected void setComplexSystematicName(IntactComplex complex) {
        // Systematic name is set after the components to have access to the gene names of the proteins
        String geneNamesConcatenated = complex.getParticipants()
                .stream()
                .map(Entity::getInteractor)
                .map(interactor -> interactor.getAliases()
                        .stream()
                        .filter(alias -> GENE_NAME_MI.equals(alias.getType().getMIIdentifier()))
                        .map(Alias::getName)
                        .findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining(":"));

        if (geneNamesConcatenated.isEmpty()) {
            // TODO: what to do if we have no gene names?
            log.warn("Systematic name could not be generated for complex " + complex.getComplexAc() + ", using Id");
            complex.setSystematicName(complex.getShortName());
        } else if (geneNamesConcatenated.length() > IntactUtils.MAX_ALIAS_NAME_LEN) {
            // TODO: MAX_ALIAS_NAME_LEN is 4000, do we want a more reasonable limit?
            log.warn("Systematic name too long for complex " + complex.getComplexAc());
            complex.setSystematicName(geneNamesConcatenated.substring(0, IntactUtils.MAX_ALIAS_NAME_LEN));
        } else {
            complex.setSystematicName(geneNamesConcatenated);
        }
    }

    private void setComplexType(IntactComplex complex) throws CvTermNotFoundException {
        IntactCvTerm interactorType = findCvTerm(IntactUtils.INTERACTOR_TYPE_OBJCLASS, STABLE_COMPLEX_MI);
        complex.setInteractorType(interactorType);
        IntactCvTerm interactionType = findCvTerm(IntactUtils.INTERACTION_TYPE_OBJCLASS, PHYSICAL_ASSOCIATION_MI);
        complex.setInteractionType(interactionType);
    }

    protected IntactCvTerm findCvTerm(String clazz, String id) throws CvTermNotFoundException {
        String key = clazz + "_" + id;
        if (cvTermMap.containsKey(key)) {
            return cvTermMap.get(key);
        }
        IntactCvTerm cvTerm = intactDao.getCvTermDao().getByUniqueIdentifier(id, clazz);
        if (cvTerm != null) {
            cvTermMap.put(key, cvTerm);
            return cvTerm;
        }
        throw new CvTermNotFoundException("CV Term not found with class '" + clazz + "' and id '" + id + "'");
    }

    protected IntactSource findSource(String id) throws SourceNotFoundException {
        if (sourceMap.containsKey(id)) {
            return sourceMap.get(id);
        }
        IntactSource source = intactDao.getSourceDao().getByMIIdentifier(id);
        if (source != null) {
            sourceMap.put(id, source);
            return source;
        }
        Collection<IntactSource> sourcesByXref = intactDao.getSourceDao().getByXref(id);
        if (sourcesByXref != null && !sourcesByXref.isEmpty()) {
            List<IntactSource> sourcesWithIdentifier = sourcesByXref.stream()
                    .filter(sourceByXref -> sourceByXref.getIdentifiers().stream().anyMatch(xrefId -> id.equals(xrefId.getId())))
                    .collect(Collectors.toList());
            if (sourcesWithIdentifier.size() == 1) {
                sourceMap.put(id, sourcesWithIdentifier.get(0));
                return sourcesWithIdentifier.get(0);
            }
        }
        throw new SourceNotFoundException("Source not found with id '" + id + "'");
    }

    private User findUser(String username) throws UserNotFoundException {
        if (userMap.containsKey(username)) {
            return userMap.get(username);
        }
        User user = intactDao.getUserDao().getByLogin(username);
        if (user != null) {
            userMap.put(username, user);
            return user;
        }
        throw new UserNotFoundException("User not found with username '" + username + "'");
    }

    private IntactOrganism findOrganism(int taxId) throws OrganismNotFoundException {
        if (organismMap.containsKey(taxId)) {
            return organismMap.get(taxId);
        }
        IntactOrganism organism = intactDao.getOrganismDao().getByTaxidOnly(taxId);
        if (organism != null) {
            organismMap.put(taxId, organism);
            return organism;
        }
        throw new OrganismNotFoundException("Organism not found with tax id '" + taxId + "'");
    }
}
