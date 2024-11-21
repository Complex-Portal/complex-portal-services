package uk.ac.ebi.complex.service.manager;

import lombok.experimental.SuperBuilder;
import psidev.psi.mi.jami.bridges.exception.BridgeFailedException;
import psidev.psi.mi.jami.model.Annotation;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.exception.OrganismNotFoundException;
import uk.ac.ebi.complex.service.exception.ProteinException;
import uk.ac.ebi.complex.service.exception.SourceNotFoundException;
import uk.ac.ebi.complex.service.exception.UserNotFoundException;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;
import uk.ac.ebi.intact.jami.model.extension.InteractorAnnotation;
import uk.ac.ebi.intact.jami.synchronizer.FinderException;
import uk.ac.ebi.intact.jami.synchronizer.PersisterException;
import uk.ac.ebi.intact.jami.synchronizer.SynchronizerException;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.util.Collection;

@SuperBuilder
public class MusicComplexManager extends ComplexManager<Double, MusicComplexToImport> {

    // TODO: update these values
    private static final String AUTHOR_CONFIDENCE_TOPIC_ID = "MI:0621";
    private static final String ML_ECO_CODE = "ECO:0008004";
    private static final String HUMAP_DATABASE_ID = "MI:2424";
    private static final String HUMAP_INSTITUION_ID = "MI:2424";

    @Override
    public IntactComplex mergeComplexWithExistingComplex(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        IntactComplex mergedComplex = super.mergeComplexWithExistingComplex(newComplex, existingComplex);
        setCellLine(newComplex, mergedComplex);
        return mergedComplex;
    }

    @Override
    public IntactComplex newComplex(MusicComplexToImport newComplex)
            throws BridgeFailedException, FinderException, SynchronizerException, PersisterException,
            SourceNotFoundException, CvTermNotFoundException, ProteinException, UserNotFoundException,
            OrganismNotFoundException {
        IntactComplex intactComplex = super.newComplex(newComplex);
        setCellLine(newComplex, intactComplex);
        return intactComplex;
    }

    @Override
    public boolean doesComplexHasIdentityXref(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        return !doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, Xref.IDENTITY_MI);
    }

    @Override
    public boolean doesComplexNeedUpdating(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        // If any of the cluster ids is missing, then the complex needs updating
        if (doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, Xref.IDENTITY_MI)) {
            return true;
        }

        // If the confidence does not match, then the complex needs updating
        Collection<Annotation> existingConfidenceAnnotations = getAuthorConfidenceAnnotations(existingComplex);
        return existingConfidenceAnnotations.stream().noneMatch(ann -> ann.getValue().equals(newComplex.getConfidence().toString()));
    }

    @Override
    public boolean doesComplexNeedSubsetXref(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        return doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, SUBSET_QUALIFIER_MI);
    }

    @Override
    public boolean doesComplexNeedComplexClusterXref(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        return doesComplexNeedXref(newComplex, existingComplex, HUMAP_DATABASE_ID, COMPLEX_CLUSTER_QUALIFIER_MI);
    }

    @Override
    protected void setComplexEvidenceType(IntactComplex complex) throws CvTermNotFoundException {
        // At the moment we are using ECO code ECO:0008004.
        // Later on we need to add support for ECO:0007653, when we import data also from ProteomeHD
        IntactCvTerm evidenceType = findCvTerm(IntactUtils.DATABASE_OBJCLASS, ML_ECO_CODE);
        complex.setEvidenceType(evidenceType);
    }

    @Override
    protected void addIdentityXrefs(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, HUMAP_DATABASE_ID, Xref.IDENTITY_MI, true);
    }

    @Override
    public IntactComplex addSubsetXrefs(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, HUMAP_DATABASE_ID, SUBSET_QUALIFIER_MI, false);
        return existingComplex;
    }

    @Override
    public IntactComplex addComplexClusterXrefs(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, HUMAP_DATABASE_ID, COMPLEX_CLUSTER_QUALIFIER_MI, false);
        return existingComplex;
    }

    @Override
    protected void addConfidenceAnnotation(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        if (newComplex.getConfidence() != null) {
            Collection<Annotation> existingConfidenceAnnotations = getAuthorConfidenceAnnotations(existingComplex);

            if (existingConfidenceAnnotations.stream().noneMatch(ann -> ann.getValue().equals(newComplex.getComplexIds().toString()))) {
                IntactCvTerm topic = findCvTerm(IntactUtils.TOPIC_OBJCLASS, AUTHOR_CONFIDENCE_TOPIC_ID);
                existingComplex.getAnnotations().add(new InteractorAnnotation(topic, newComplex.getConfidence().toString()));
            }
        }
    }

    @Override
    protected void setComplexSource(IntactComplex complex) throws SourceNotFoundException {
        IntactSource source = findSource(HUMAP_INSTITUION_ID);
        complex.setSource(source);
    }

    @Override
    protected void addNames(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        setComplexSystematicName(existingComplex);
        if (newComplex.getName() != null && !newComplex.getName().isEmpty()) {
            // TODO: verify which name to use
            // TODO: should we add some sort of name for merged complexes?
            existingComplex.setRecommendedName(newComplex.getName());
        }
    }

    private void setCellLine(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        // TODO: ass cell line xref
    }
}
