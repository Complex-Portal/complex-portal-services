package uk.ac.ebi.complex.service.manager;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
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
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.ComplexAcValue;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactModelledParticipant;
import uk.ac.ebi.intact.jami.model.extension.IntactOrganism;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;
import uk.ac.ebi.intact.jami.model.extension.IntactStoichiometry;
import uk.ac.ebi.intact.jami.model.extension.InteractorAnnotation;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Log4j
@Component
@RequiredArgsConstructor
public class UniplexComplexManager {

    private static final String STABLE_COMPLEX_MI = "MI:1302";
    private static final String PHYSICAL_ASSOCIATION_MI = "MI:0915";
    private static final String GENE_NAME_MI = "MI:0301";
    private final static String AUTHOR_CONFIDENCE_TOPIC_ID = "MI:0621";
    private static final String ML_ECO_CODE = "ECO:0008004";
    private final static String HUMAP_DATABASE_ID = "MI:2424";
    private final static String HUMAP_INSTITUION_ID = "MI:2424";

    private static final Integer HUMAN_TAX_ID = 9606;
    private static final String READY_FOR_RELEASE_COMPLEX_PUBMED_ID = "14681455";

    private final IntactDao intactDao;
    private final UniprotProteinFetcher uniprotProteinFetcher;
    private final AppProperties appProperties;

    private final Map<String, IntactCvTerm> cvTermMap = new HashMap<>();
    private final Map<String, IntactSource> sourceMap = new HashMap<>();
    private final Map<String, IntactProtein> proteinMap = new HashMap<>();
    private final Map<String, User> userMap = new HashMap<>();
    private final Map<Integer, IntactOrganism> organismMap = new HashMap<>();

    public IntactComplex mergeClusterWithExistingComplex(UniplexCluster uniplexCluster, IntactComplex complex) throws CvTermNotFoundException {
        addHumapXrefs(uniplexCluster, complex);
        addConfidenceAnnotation(uniplexCluster, complex);
        return complex;
    }

    public IntactComplex newComplexFromCluster(UniplexCluster uniplexCluster)
            throws BridgeFailedException, FinderException, SynchronizerException, PersisterException,
            SourceNotFoundException, CvTermNotFoundException, ProteinException, UserNotFoundException,
            OrganismNotFoundException {

        IntactComplex complex = new IntactComplex(uniplexCluster.getClusterIds().iterator().next());
        setComplexAc(complex);
        setOrganism(complex);
        setComplexSource(complex);
        setComplexType(complex);
        setComplexEvidenceType(complex);
        addHumapXrefs(uniplexCluster, complex);
        addConfidenceAnnotation(uniplexCluster, complex);
        setComplexComponents(uniplexCluster, complex);
        // Systematic name is set after the components to have access to the gene names of the proteins
        setComplexSystematicName(complex);
        setComplexStatus(complex);
        setExperimentAndPublication(complex);
        complex.setPredictedComplex(true);
        complex.setCreatedDate(new Date());
        complex.setUpdatedDate(complex.getCreatedDate());

        return complex;
    }

    private void setExperimentAndPublication(IntactComplex complex) {
        IntactUtils.createAndAddDefaultExperimentForComplexes(complex, READY_FOR_RELEASE_COMPLEX_PUBMED_ID);
    }

    private void setOrganism(IntactComplex complex) throws OrganismNotFoundException {
        // Currently we are only importing human complexes.
        // If we want to add support for other organisms, we need to update this.
        complex.setOrganism(findOrganism(HUMAN_TAX_ID));
    }

    private void setComplexEvidenceType(IntactComplex complex) throws CvTermNotFoundException {
        // At the moment we are using ECO code ECO:0008004.
        // Later on we need to add support for ECO:0007653, when we import data also from ProteomeHD
        IntactCvTerm evidenceType = findCvTerm(IntactUtils.DATABASE_OBJCLASS, ML_ECO_CODE);
        complex.setEvidenceType(evidenceType);
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

    private void setComplexComponents(UniplexCluster uniplexCluster, IntactComplex complex) throws BridgeFailedException, FinderException, SynchronizerException, PersisterException, ProteinException {
        for (String uniprotAc: uniplexCluster.getUniprotAcs()) {
            IntactProtein intactProtein = getIntactProtein(uniprotAc);
            complex.getParticipants().add(new IntactModelledParticipant(intactProtein, new IntactStoichiometry(0)));
        }
    }

    private void addHumapXrefs(UniplexCluster uniplexCluster, IntactComplex complex) throws CvTermNotFoundException {
        Collection<Xref> existingHumapXrefs = complex.getIdentifiers()
                .stream()
                .filter(id -> HUMAP_DATABASE_ID.equals(id.getDatabase().getMIIdentifier()))
                .filter(id -> Xref.IDENTITY_MI.equals(id.getQualifier().getMIIdentifier()))
                .collect(Collectors.toList());
        for (String clusterId: uniplexCluster.getClusterIds()) {
            if (existingHumapXrefs.stream().noneMatch(id -> id.getId().equals(clusterId))) {
                InteractorXref xref = newHumapXref(clusterId);
                // We add the new xrefs to identifiers as we are using the identity qualifier.
                // If we eventually use another qualifier, we should add them to xrefs.
                complex.getIdentifiers().add(xref);
            }
        }
        // TODO: what to do with existing old humap xrefs no longer valid?
    }

    private InteractorXref newHumapXref(String id) throws CvTermNotFoundException {
        IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, HUMAP_DATABASE_ID);
        // Currently we use identity as qualifier, as we are only importing exact matches.
        // If we merge curated complexes with partial matches, we need to add a different qualifier (subset, see-also, etc.).
        IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.IDENTITY_MI);
        return new InteractorXref(database, id, qualifier);
    }

    private void addConfidenceAnnotation(UniplexCluster uniplexCluster, IntactComplex complex) throws CvTermNotFoundException {
        Collection<Annotation> existingConfidenceAnnotations = complex.getAnnotations()
                .stream()
                .filter(ann -> AUTHOR_CONFIDENCE_TOPIC_ID.equals(ann.getTopic().getMIIdentifier()))
                .collect(Collectors.toList());

        if (existingConfidenceAnnotations.stream().noneMatch(ann -> ann.getValue().equals(uniplexCluster.getClusterConfidence().toString()))) {
            IntactCvTerm topic = findCvTerm(IntactUtils.TOPIC_OBJCLASS, AUTHOR_CONFIDENCE_TOPIC_ID);
            complex.getAnnotations().add(new InteractorAnnotation(topic, uniplexCluster.getClusterConfidence().toString()));
        }
        // TODO: what to do with existing old humap confidence annotations no longer valid?
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

    private void setComplexSystematicName(IntactComplex complex) {
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
            log.warn("Systematic name could not be generated for complex " + complex.getComplexAc() + ", using cluster Id");
            complex.setSystematicName(complex.getShortName());
        } else if (geneNamesConcatenated.length() > IntactUtils.MAX_ALIAS_NAME_LEN) {
            // TODO: MAX_ALIAS_NAME_LEN is 4000, do we want a more reasonable limit?
            log.warn("Systematic name too long for complex " + complex.getComplexAc());
            complex.setSystematicName(geneNamesConcatenated.substring(0, IntactUtils.MAX_ALIAS_NAME_LEN));
        } else {
            complex.setSystematicName(geneNamesConcatenated);
        }
    }

    private void setComplexSource(IntactComplex complex) throws SourceNotFoundException {
        // At the moment we are only importing huMAP clusters.
        // When we also import ProteomeHD data, we need to review this.
        IntactSource source = findSource(HUMAP_INSTITUION_ID);
        complex.setSource(source);
    }

    private void setComplexType(IntactComplex complex) throws CvTermNotFoundException {
        IntactCvTerm interactorType = findCvTerm(IntactUtils.INTERACTOR_TYPE_OBJCLASS, STABLE_COMPLEX_MI);
        complex.setInteractorType(interactorType);
        IntactCvTerm interactionType = findCvTerm(IntactUtils.INTERACTION_TYPE_OBJCLASS, PHYSICAL_ASSOCIATION_MI);
        complex.setInteractionType(interactionType);
    }

    private IntactCvTerm findCvTerm(String clazz, String id) throws CvTermNotFoundException {
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

    private IntactSource findSource(String id) throws SourceNotFoundException {
        if (sourceMap.containsKey(id)) {
            return sourceMap.get(id);
        }
        IntactSource source = intactDao.getSourceDao().getByMIIdentifier(id);
        if (source != null) {
            sourceMap.put(id, source);
            return source;
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
