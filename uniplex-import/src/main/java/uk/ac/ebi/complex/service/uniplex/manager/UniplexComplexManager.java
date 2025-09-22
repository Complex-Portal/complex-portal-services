package uk.ac.ebi.complex.service.uniplex.manager;

import lombok.experimental.SuperBuilder;
import psidev.psi.mi.jami.model.Annotation;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.batch.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.batch.exception.SourceNotFoundException;
import uk.ac.ebi.complex.service.batch.manager.ComplexManager;
import uk.ac.ebi.complex.service.uniplex.model.UniplexCluster;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;
import uk.ac.ebi.intact.jami.model.extension.InteractorAnnotation;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.util.Collection;

@SuperBuilder
public class UniplexComplexManager extends ComplexManager<Integer, UniplexCluster> {

    public static final String HUMAP_DATABASE_ID = "MI:2424";
    private static final String HUMAP_INSTITUION_ID = "MI:2424";

    @Override
    public boolean doesComplexHasIdentityXref(UniplexCluster newComplex, IntactComplex existingComplex) {
        return !doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, Xref.IDENTITY_MI);
    }

    @Override
    public boolean doesComplexNeedUpdating(UniplexCluster newComplex, IntactComplex existingComplex) {
        // If any of the cluster ids is missing, then the complex needs updating
        if (doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, Xref.IDENTITY_MI)) {
            return true;
        }

        // If the confidence does not match, then the complex needs updating
        Collection<Annotation> existingConfidenceAnnotations = getAuthorConfidenceAnnotations(existingComplex);
        if (existingConfidenceAnnotations.stream().noneMatch(ann -> ann.getValue().equals(newComplex.getConfidence().toString()))) {
            return true;
        }

        return doesComplexEcoCodeNeedUpdating(existingComplex);
    }

    @Override
    public boolean doesComplexNeedSubsetXref(UniplexCluster newComplex, IntactComplex existingComplex) {
        return doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, SUBSET_QUALIFIER_MI);
    }

    @Override
    public boolean doesComplexNeedComplexClusterXref(UniplexCluster newComplex, IntactComplex existingComplex) {
        return doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, COMPLEX_CLUSTER_QUALIFIER_MI);
    }

    @Override
    protected void addIdentityXrefs(UniplexCluster newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, HUMAP_DATABASE_ID, Xref.IDENTITY_MI, true);
    }

    @Override
    public IntactComplex addSubsetXrefs(UniplexCluster newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, HUMAP_DATABASE_ID, SUBSET_QUALIFIER_MI, false);
        return existingComplex;
    }

    @Override
    public IntactComplex addComplexClusterXrefs(UniplexCluster newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, HUMAP_DATABASE_ID, COMPLEX_CLUSTER_QUALIFIER_MI, false);
        return existingComplex;
    }

    @Override
    protected void addConfidenceAnnotation(UniplexCluster newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        Collection<Annotation> existingConfidenceAnnotations = getAuthorConfidenceAnnotations(existingComplex);

        if (existingConfidenceAnnotations.stream().noneMatch(ann -> ann.getValue().equals(newComplex.getConfidence().toString()))) {
            IntactCvTerm topic = findCvTerm(IntactUtils.TOPIC_OBJCLASS, AUTHOR_CONFIDENCE_TOPIC_ID);
            existingComplex.getAnnotations().add(new InteractorAnnotation(topic, newComplex.getConfidence().toString()));
        }
    }

    @Override
    protected void setComplexSource(IntactComplex complex) throws SourceNotFoundException {
        // At the moment we are only importing huMAP clusters.
        // When we also import ProteomeHD data, we need to review this.
        IntactSource source = findSource(HUMAP_INSTITUION_ID);
        complex.setSource(source);
    }
}
