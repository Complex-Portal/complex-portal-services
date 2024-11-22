package uk.ac.ebi.complex.service.manager;

import lombok.experimental.SuperBuilder;
import psidev.psi.mi.jami.bridges.exception.BridgeFailedException;
import psidev.psi.mi.jami.model.Alias;
import psidev.psi.mi.jami.model.Annotation;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.AliasUtils;
import uk.ac.ebi.complex.service.config.MusicImportAppProperties;
import uk.ac.ebi.complex.service.exception.CvTermNotFoundException;
import uk.ac.ebi.complex.service.exception.OrganismNotFoundException;
import uk.ac.ebi.complex.service.exception.ProteinException;
import uk.ac.ebi.complex.service.exception.SourceNotFoundException;
import uk.ac.ebi.complex.service.exception.UserNotFoundException;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactCvTerm;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;
import uk.ac.ebi.intact.jami.model.extension.InteractorAlias;
import uk.ac.ebi.intact.jami.model.extension.InteractorAnnotation;
import uk.ac.ebi.intact.jami.synchronizer.FinderException;
import uk.ac.ebi.intact.jami.synchronizer.PersisterException;
import uk.ac.ebi.intact.jami.synchronizer.SynchronizerException;
import uk.ac.ebi.intact.jami.utils.IntactUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuperBuilder
public class MusicComplexManager extends ComplexManager<Double, MusicComplexToImport> {

    private static final String AUTHOR_CONFIDENCE_TOPIC_ID = "MI:0621";
    private static final String ML_ECO_CODE = "ECO:0008004";
    private static final String EFO_DATABASE_ID = "MI:1337";
    private static final String MUSIC_DATABASE_ID = "IA:3605";
    private static final String MUSIC_INSTITUION_ID = "IA:3605";

    private final MusicImportAppProperties musicImportAppProperties;

    @Override
    public IntactComplex mergeComplexWithExistingComplex(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        IntactComplex mergedComplex = super.mergeComplexWithExistingComplex(newComplex, existingComplex);

        List<Alias> synonyms = new ArrayList<>(AliasUtils.collectAllAliasesHavingType(mergedComplex.getAliases(), Alias.COMPLEX_SYNONYM_MI, Alias.COMPLEX_SYNONYM));
        if (newComplex.getName() != null && !newComplex.getName().isEmpty()) {
            if (synonyms.stream().noneMatch(synonym -> newComplex.getName().equals(synonym.getName()))) {
                IntactCvTerm complexSynonymTerm = findCvTerm(IntactUtils.ALIAS_TYPE_OBJCLASS, Alias.COMPLEX_SYNONYM_MI);
                Alias newSynonym = new InteractorAlias(complexSynonymTerm, newComplex.getName());
                mergedComplex.getAliases().add(newSynonym);
            }
        }
        setExtraXrefs(mergedComplex);

        return mergedComplex;
    }

    @Override
    public IntactComplex newComplex(MusicComplexToImport newComplex)
            throws BridgeFailedException, FinderException, SynchronizerException, PersisterException,
            SourceNotFoundException, CvTermNotFoundException, ProteinException, UserNotFoundException,
            OrganismNotFoundException {
        IntactComplex intactComplex = super.newComplex(newComplex);

        if (newComplex.getName() != null && !newComplex.getName().isEmpty()) {
            intactComplex.setRecommendedName(newComplex.getName());
        }
        setExtraXrefs(intactComplex);

        return intactComplex;
    }

    @Override
    public boolean doesComplexHasIdentityXref(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        return !doesComplexNeedXref(newComplex, existingComplex, MUSIC_DATABASE_ID, Xref.IDENTITY_MI);
    }

    @Override
    public boolean doesComplexNeedUpdating(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        // If any of the cluster ids is missing, then the complex needs updating
        if (doesComplexNeedXref(newComplex, existingComplex, MUSIC_DATABASE_ID, Xref.IDENTITY_MI)) {
            return true;
        }

        if (newComplex.getConfidence() != null) {
            Collection<Annotation> existingConfidenceAnnotations = getAuthorConfidenceAnnotations(existingComplex);
            if (existingConfidenceAnnotations.stream().noneMatch(ann -> ann.getValue().equals(newComplex.getConfidence().toString()))) {
                return true;
            }
        }

        if (musicImportAppProperties.getCellLine() != null && !musicImportAppProperties.getCellLine().isEmpty()) {
            if (doesComplexNeedXref(musicImportAppProperties.getCellLine(), existingComplex, EFO_DATABASE_ID, Xref.IDENTITY_MI)) {
                return true;
            }
        }

        if (musicImportAppProperties.getPublicationId() != null && !musicImportAppProperties.getPublicationId().isEmpty()) {
            return doesComplexNeedXref(musicImportAppProperties.getPublicationId(), existingComplex, Xref.PUBMED_MI, Xref.SEE_ALSO_MI);
        }

        return false;
    }

    @Override
    public boolean doesComplexNeedSubsetXref(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        return doesComplexNeedXref(newComplex, existingComplex, MUSIC_DATABASE_ID, SUBSET_QUALIFIER_MI);
    }

    @Override
    public boolean doesComplexNeedComplexClusterXref(MusicComplexToImport newComplex, IntactComplex existingComplex) {
        return doesComplexNeedXref(newComplex, existingComplex, MUSIC_DATABASE_ID, COMPLEX_CLUSTER_QUALIFIER_MI);
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
        addXrefs(newComplex, existingComplex, MUSIC_DATABASE_ID, Xref.IDENTITY_MI, true);
    }

    @Override
    public IntactComplex addSubsetXrefs(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, MUSIC_DATABASE_ID, SUBSET_QUALIFIER_MI, false);
        return existingComplex;
    }

    @Override
    public IntactComplex addComplexClusterXrefs(MusicComplexToImport newComplex, IntactComplex existingComplex) throws CvTermNotFoundException {
        addXrefs(newComplex, existingComplex, MUSIC_DATABASE_ID, COMPLEX_CLUSTER_QUALIFIER_MI, false);
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
        IntactSource source = findSource(MUSIC_INSTITUION_ID);
        complex.setSource(source);
    }

    private void setExtraXrefs(IntactComplex existingComplex) throws CvTermNotFoundException {
        if (musicImportAppProperties.getCellLine() != null && !musicImportAppProperties.getCellLine().isEmpty()) {
            addXrefs(musicImportAppProperties.getCellLine(), existingComplex, EFO_DATABASE_ID, Xref.IDENTITY_MI, false);
        }
        if (musicImportAppProperties.getPublicationId() != null && !musicImportAppProperties.getPublicationId().isEmpty()) {
            addXrefs(musicImportAppProperties.getPublicationId(), existingComplex, Xref.PUBMED_MI, Xref.SEE_ALSO_MI, false);
        }
    }
}
