package uk.ac.ebi.complex.service;

import lombok.RequiredArgsConstructor;
import psidev.psi.mi.jami.bridges.exception.BridgeFailedException;
import psidev.psi.mi.jami.bridges.uniprot.UniprotProteinFetcher;
import psidev.psi.mi.jami.model.Annotation;
import psidev.psi.mi.jami.model.Entity;
import psidev.psi.mi.jami.model.Protein;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.ComplexAcValue;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactModelledParticipant;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;
import uk.ac.ebi.intact.jami.model.extension.InteractorAnnotation;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;
import uk.ac.ebi.intact.jami.model.lifecycle.ComplexLifeCycleEvent;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleEventType;
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

@RequiredArgsConstructor
public class UniplexComplexManager {

    // TODO: this MI id is for topic 'comment'. Replace with new topic for humap confidence
    private final static String CONFIDENCE_TOPIC_MI = "MI:0612";
    // TODO: this MI id is for database 'intact'. Replace with new database for humap
    private final static String HUMAP_DATABASE_MI = "MI:0469";
    // TODO: this MI id is for database 'see-also'. Replace with new qualifier
    private final static String QUALIFIER_MI = "MI:0361";
    // TODO: this MI id is for instituion 'IntAct'. Replace with new institution for huMap
    private final static String HUMAP_INSTITUTION_MI = "MI:0469";

    private static final String STABLE_COMPLEX_MI = "MI:1302";
    private static final String PHYSICAL_ASSOCIATION_MI = "MI:0915";
    private static final String GENE_NAME_MI = "MI:0301";

    // TODO: should be ECO:0008004, but it's not yet created in the DB
    private static final String ML_ECO_CODE = "ECO:0000353";

    // TODO: this needs to be updated to an agreed creator, or we should take it as an input parameter to the job
    private static final String CREATOR_USERNAME = "jmedina";

    private final IntactDao intactDao;
    private final UniprotProteinFetcher uniprotProteinFetcher;

    private final Map<String, IntactCvTerm> cvTermMap = new HashMap<>();
    private final Map<String, IntactSource> sourceMap = new HashMap<>();
    private final Map<String, IntactProtein> proteinMap = new HashMap<>();
    private final Map<String, User> userMap = new HashMap<>();

    public IntactComplex mergeClusterWithExistingComplex(UniplexCluster uniplexCluster, IntactComplex complex) {
        addHumapXrefs(uniplexCluster, complex);
        addConfidenceAnnotation(uniplexCluster, complex);
        return complex;
    }

    public IntactComplex newComplexFromCluster(UniplexCluster uniplexCluster) throws BridgeFailedException, FinderException, SynchronizerException, PersisterException {
        // TODO: what whould the short name be? Using cluster id for now
        IntactComplex complex = new IntactComplex(uniplexCluster.getClusterIds().iterator().next());
        setComplexAc(complex);
        setComplexSource(complex);
        setComplexType(complex);
        setEvidenceType(complex);
        addHumapXrefs(uniplexCluster, complex);
        addConfidenceAnnotation(uniplexCluster, complex);
        setComplexComponents(uniplexCluster, complex);
        // Systematic name is set after the components to have access to the gene names of the proteins
        setComplexSystematicName(complex);
        setComplexStatus(complex);
        complex.setCreatedDate(new Date());
        complex.setUpdatedDate(complex.getCreatedDate());

        // TODO: evidence code
        // TODO: flag or something to indicate it's a predicted complex

        return complex;
    }

    private void setEvidenceType(IntactComplex complex) {
        IntactCvTerm evidenceType = findCvTerm(IntactUtils.DATABASE_OBJCLASS, ML_ECO_CODE);
        if (evidenceType != null) {
            complex.setEvidenceType(evidenceType);
        } else {
            // TODO: should we throw an error here?
        }
    }

    private void setComplexStatus(IntactComplex complex) {
        User user = findUser(CREATOR_USERNAME);
        if (user != null) {
            complex.setCreator(user.getLogin());
            complex.setUpdator(user.getLogin());

            complex.getLifecycleEvents().add(
                    new ComplexLifeCycleEvent(LifeCycleEventType.CREATED, user, "New predicted complex"));
            complex.getLifecycleEvents().add(
                    new ComplexLifeCycleEvent(LifeCycleEventType.READY_FOR_RELEASE, user, "Predicted complex read for release"));

        } else {
            // TODO: should we throw an error here?
        }
    }

    private IntactProtein getIntactProtein(String proteinId) throws BridgeFailedException, FinderException, SynchronizerException, PersisterException {
        if (proteinMap.containsKey(proteinId)) {
            return proteinMap.get(proteinId);
        }
        Collection<IntactProtein> proteinByXref = intactDao.getProteinDao().getByXrefQualifier(Xref.IDENTITY, Xref.IDENTITY_MI, proteinId);
        if (!proteinByXref.isEmpty()) {
            if (proteinByXref.size() == 1) {
                return proteinByXref.iterator().next();
            }
            // TODO: should we log something if we get more than 1 protein?
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
            }
            // TODO: should we log something if we get more than 1 protein or no protein at all?
        }
        return null;
    }

    private void setComplexComponents(UniplexCluster uniplexCluster, IntactComplex complex) throws BridgeFailedException, FinderException, SynchronizerException, PersisterException {
        for (String uniprotAc: uniplexCluster.getUniprotAcs()) {
            IntactProtein intactProtein = getIntactProtein(uniprotAc);
            if (intactProtein != null) {
                complex.getParticipants().add(new IntactModelledParticipant(intactProtein));
            } else {
                // TODO: should we throw an error here?
            }
        }
    }

    private void addHumapXrefs(UniplexCluster uniplexCluster, IntactComplex complex) {
        for (String clusterId: uniplexCluster.getClusterIds()) {
            InteractorXref xref = newHumapXref(clusterId);
            if (xref != null) {
                complex.getXrefs().add(xref);
            } else {
                // TODO: should we throw an error here?
            }
        }
    }

    private InteractorXref newHumapXref(String id) {
        IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, HUMAP_DATABASE_MI);
        IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, QUALIFIER_MI);
        // TODO: in future versions we may need to increase the version
        String version = "1";
        if (database != null && qualifier != null) {
            return new InteractorXref(database, id, version, qualifier);
            // TODO: add ECO code to humap xref. This will need a new class in intact-jami, similar to ComplexGOXref
        }
        return null;
    }

    private void addConfidenceAnnotation(UniplexCluster uniplexCluster, IntactComplex complex) {
        IntactCvTerm topic = findCvTerm(IntactUtils.TOPIC_OBJCLASS, CONFIDENCE_TOPIC_MI);
        if (topic != null) {
            complex.getAnnotations().add(new InteractorAnnotation(topic, uniplexCluster.getClusterConfidence().toString()));
        } else {
            // TODO: should we throw an error here?
        }
    }

    private void setComplexAc(IntactComplex complex) {
        InteractorXref xref = newComplexAc(complex);
        if (xref != null) {
            complex.getIdentifiers().add(xref);
        } else {
            // TODO: should we throw an error here?
        }
    }

    private InteractorXref newComplexAc(IntactComplex complex) {
        IntactCvTerm database = findCvTerm(IntactUtils.DATABASE_OBJCLASS, Xref.COMPLEX_PORTAL_MI);
        IntactCvTerm qualifier = findCvTerm(IntactUtils.QUALIFIER_OBJCLASS, Xref.COMPLEX_PRIMARY_MI);
        // TODO: in future versions we may need to increase the version
        String version = "1";
        if (database != null && qualifier != null) {
            String acValue = ComplexAcValue.getNextComplexAcValue(intactDao.getEntityManager());
            if (acValue != null) {
                return new InteractorXref(database, acValue, version, qualifier);
            }
        }
        return null;
    }

    private void setComplexSystematicName(IntactComplex complex) {
        String geneNamesConcatenated = complex.getParticipants()
                .stream()
                .map(Entity::getInteractor)
                .map(interactor -> interactor.getAnnotations()
                        .stream()
                        .filter(ann -> GENE_NAME_MI.equals(ann.getTopic().getMIIdentifier()))
                        .map(Annotation::getValue)
                        .findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining("_"));

        if (geneNamesConcatenated.isEmpty()) {
            // TODO: what to do if we have no gene names?
        } else if (geneNamesConcatenated.length() > IntactUtils.MAX_ALIAS_NAME_LEN) {
            // TODO: MAX_ALIAS_NAME_LEN is 4000, do we want a more reasonable limit?
            complex.setSystematicName(geneNamesConcatenated.substring(0, IntactUtils.MAX_ALIAS_NAME_LEN));
        } else {
            complex.setSystematicName(geneNamesConcatenated);
        }
    }

    private void setComplexSource(IntactComplex complex) {
        IntactSource source = findSource(HUMAP_INSTITUTION_MI);
        if (source != null) {
            complex.setSource(source);
        } else {
            // TODO: should we throw an error here?
        }
    }

    private void setComplexType(IntactComplex complex) {
        IntactCvTerm interactorType = findCvTerm(IntactUtils.INTERACTOR_TYPE_OBJCLASS, STABLE_COMPLEX_MI);
        if (interactorType != null) {
            complex.setInteractorType(interactorType);
        } else {
            // TODO: should we throw an error here?
        }
        IntactCvTerm interactionType = findCvTerm(IntactUtils.INTERACTION_TYPE_OBJCLASS, PHYSICAL_ASSOCIATION_MI);
        if (interactionType != null) {
            complex.setInteractionType(interactionType);
        } else {
            // TODO: should we throw an error here?
        }
    }

    private IntactCvTerm findCvTerm(String clazz, String id) {
        String key = clazz + "_" + id;
        if (cvTermMap.containsKey(key)) {
            return cvTermMap.get(key);
        }
        IntactCvTerm cvTerm = intactDao.getCvTermDao().getByUniqueIdentifier(id, clazz);
        if (cvTerm != null) {
            cvTermMap.put(key, cvTerm);
            return cvTerm;
        }
        return null;
    }

    private IntactSource findSource(String id) {
        if (sourceMap.containsKey(id)) {
            return sourceMap.get(id);
        }
        IntactSource source = intactDao.getSourceDao().getByMIIdentifier(id);
        if (source != null) {
            sourceMap.put(id, source);
            return source;
        }
        return null;
    }

    private User findUser(String username) {
        if (userMap.containsKey(username)) {
            return userMap.get(username);
        }
        User user = intactDao.getUserDao().getByLogin(username);
        if (user != null) {
            userMap.put(username, user);
            return user;
        }
        return null;
    }
}
