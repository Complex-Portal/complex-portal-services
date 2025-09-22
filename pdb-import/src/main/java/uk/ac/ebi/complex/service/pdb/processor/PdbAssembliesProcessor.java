package uk.ac.ebi.complex.service.pdb.processor;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.ModelledComparableParticipant;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.model.impl.DefaultCvTerm;
import psidev.psi.mi.jami.model.impl.DefaultXref;
import psidev.psi.mi.jami.utils.CvTermUtils;
import psidev.psi.mi.jami.utils.XrefUtils;
import psidev.psi.mi.jami.utils.comparator.CollectionComparator;
import psidev.psi.mi.jami.utils.comparator.participant.ModelledComparableParticipantComparator;
import uk.ac.ebi.complex.service.batch.manager.ComplexManager;
import uk.ac.ebi.complex.service.batch.processor.AbstractBatchProcessor;
import uk.ac.ebi.complex.service.pdb.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.pdb.logging.ProcessReportWriter;
import uk.ac.ebi.complex.service.pdb.model.AssemblyEntry;
import uk.ac.ebi.complex.service.pdb.model.ComplexWithAssemblies;
import uk.ac.ebi.complex.service.pdb.model.ComplexWithAssemblyXrefs;
import uk.ac.ebi.complex.service.pdb.reader.PdbAssembliesFileReader;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.IntactProtein;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@SuperBuilder
public class PdbAssembliesProcessor extends AbstractBatchProcessor<ComplexWithAssemblies, ComplexWithAssemblyXrefs> {

    private static final String EXP_EVIDENCE = "exp-evidence";

    private static final String ML_ECO_CODE = "ECO:0008004";
    private static final String COMP_EVIDENCE_ECO_CODE = "ECO:0007653";

    private final IntactDao intactDao;
    private final PdbAssembliesFileReader pdbAssembliesFileReader;

    private ProcessReportWriter noChangesReportWriter;
    private ProcessReportWriter complexesMissingAssembliesReportWriter;
    private ProcessReportWriter complexesWithExtraAssembliesReportWriter;
    private ProcessReportWriter complexesWithXrefsToUpdateReportWriter;
    private ProcessReportWriter complexesWithXrefsToReviewReportWriter;
    private ProcessReportWriter ecoCodeChangesReportWriter;
    private ErrorsReportWriter errorReportWriter;

    private CollectionComparator<ModelledComparableParticipant> comparableParticipantsComparator;
    private Map<String, IntactProtein> proteinCacheMap;
    private Set<AssemblyEntry> assemblies;

    @Override
    public ComplexWithAssemblyXrefs process(ComplexWithAssemblies item) throws Exception {
        try {
            IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(item.getComplexId());
            List<Xref> pdbXrefs = new ArrayList<>();
            pdbXrefs.addAll(XrefUtils.collectAllXrefsHavingDatabase(
                    complex.getIdentifiers(), ComplexManager.WWPDB_DB_MI, ComplexManager.WWPDB_DB_NAME));
            pdbXrefs.addAll(XrefUtils.collectAllXrefsHavingDatabase(
                    complex.getXrefs(), ComplexManager.WWPDB_DB_MI, ComplexManager.WWPDB_DB_NAME));

            List<String> xrefsToAdd = new ArrayList<>();
            List<InteractorXref> xrefsToRemove = new ArrayList<>();
            List<InteractorXref> xrefsToUpdate = new ArrayList<>();
            List<InteractorXref> xrefsToReview = new ArrayList<>();

            Set<String> matchesFound = new HashSet<>();

            item.getAssembliesFromFile().forEach(assembly -> checkIfAssemblyMatchesAnyXref(
                    pdbXrefs, assembly, matchesFound, xrefsToAdd, xrefsToUpdate, xrefsToReview));

            Collection<ModelledComparableParticipant> complexProteins = getProteinComponents(complex, proteinCacheMap);

            Set<String> assembliesToCheck = new HashSet<>();
            for (AssemblyEntry assemblyEntry : assemblies) {
                Collection<ModelledComparableParticipant> assemblyProteins = assemblyEntry.getProteins().stream()
                        .filter(protein ->
                                protein.getOrganism() == null || protein.getOrganism() == complex.getOrganism().getTaxId())
                        .map(protein -> new ModelledComparableParticipant(
                                protein.getProteinAc(),
                                List.of(new DefaultXref(
                                        new DefaultCvTerm(Xref.UNIPROTKB, Xref.UNIPROTKB_MI),
                                        protein.getProteinAc(),
                                        new DefaultCvTerm(Xref.IDENTITY, Xref.IDENTITY_MI))),
                                1,
                                CvTermUtils.createProteinInteractorType()))
                        .collect(Collectors.toList());

                if (this.comparableParticipantsComparator.compare(complexProteins, assemblyProteins) == 0) {
                    assemblyEntry.getAssemblies().forEach(assembly -> assembliesToCheck.add(assembly.toLowerCase()));
                }
            }

            for (Xref xref: pdbXrefs) {
                if (xref.getQualifier() != null) {
                    if (!matchesFound.contains(((InteractorXref) xref).getAc())) {
                        if (!assembliesToCheck.contains(xref.getId().toLowerCase())) {
                            if (Xref.IDENTITY_MI.equals(xref.getQualifier().getMIIdentifier())) {
                                if (item.getAssembliesWithSameProteins().contains(xref.getId().toLowerCase())) {
                                    matchesFound.add(((InteractorXref) xref).getAc());
                                } else {
                                    // Xref does not match any PDB assembly, we delete it as it has identity qualifier
                                    xrefsToRemove.add((InteractorXref) xref);
                                }
                            } else if (EXP_EVIDENCE.equals(xref.getQualifier().getShortName())) {
                                xrefsToReview.add((InteractorXref) xref);
                            }
                        }
                    }
                }
            }

            if (xrefsToAdd.isEmpty() && xrefsToRemove.isEmpty() && xrefsToUpdate.isEmpty() && xrefsToReview.isEmpty()) {
                noChangesReportWriter.write(
                        item.getComplexId(),
                        item.isPredicted(),
                        pdbXrefs.stream().map(Xref::getId).collect(Collectors.toSet()));
            } else if (!xrefsToAdd.isEmpty() && xrefsToRemove.isEmpty() && xrefsToUpdate.isEmpty() && xrefsToReview.isEmpty()) {
                complexesMissingAssembliesReportWriter.write(
                        item.getComplexId(),
                        item.isPredicted(),
                        item.getAssembliesFromFile(),
                        xrefsToAdd);
            } else if (!xrefsToRemove.isEmpty() && xrefsToAdd.isEmpty() && xrefsToUpdate.isEmpty() && xrefsToReview.isEmpty()) {
                complexesWithExtraAssembliesReportWriter.write(
                        item.getComplexId(),
                        item.isPredicted(),
                        item.getAssembliesFromFile(),
                        xrefsToRemove.stream()
                                .map(xref -> xref.getId() + "(" + (xref.getQualifier() != null ? xref.getQualifier().getShortName() : "" ) + ")")
                                .collect(Collectors.toList()));
            } else if (!xrefsToUpdate.isEmpty() && xrefsToAdd.isEmpty() && xrefsToRemove.isEmpty() && xrefsToReview.isEmpty()) {
                complexesWithXrefsToUpdateReportWriter.write(
                        item.getComplexId(),
                        item.isPredicted(),
                        item.getAssembliesFromFile(),
                        xrefsToUpdate.stream()
                                .map(xref -> xref.getId() + "(" + (xref.getQualifier() != null ? xref.getQualifier().getShortName() : "" ) + ")")
                                .collect(Collectors.toList()));
            }  else {
                complexesWithXrefsToReviewReportWriter.write(
                        item.getComplexId(),
                        item.isPredicted(),
                        item.getAssembliesFromFile(),
                        pdbXrefs.stream()
                                .map(xref -> xref.getId() + "(" + (xref.getQualifier() != null ? xref.getQualifier().getShortName() : "" ) + ")")
                                .collect(Collectors.toList()));
            }

            if (complex.isPredictedComplex()) {
                String expectedEcoCode = matchesFound.isEmpty() ? ComplexManager.ML_ECO_CODE : ComplexManager.COMP_EVIDENCE_ECO_CODE;
                Set<String> complexEcoCodes = XrefUtils.collectAllXrefsHavingDatabase(complex.getEvidenceType().getIdentifiers(), "MI:1331", "evidence ontology")
                        .stream()
                        .map(Xref::getId)
                        .collect(Collectors.toSet());
                if (!complexEcoCodes.contains(expectedEcoCode)) {
                    ecoCodeChangesReportWriter.write(
                            item.getComplexId(),
                            item.isPredicted(),
                            String.join("|", complexEcoCodes),
                            expectedEcoCode);
                }
            }

            return new ComplexWithAssemblyXrefs(
                    item.getComplexId(),
                    xrefsToAdd,
                    xrefsToRemove,
                    xrefsToUpdate);

        } catch (Exception e) {
            log.error("Error processing assemblies for complex: " + item.getComplexId(), e);
            errorReportWriter.write(item.getComplexId(), e.getMessage());
            return null;
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);

        try {
            ModelledComparableParticipantComparator participantComparator = new ModelledComparableParticipantComparator();
            // Ignore stoichiometry for now
            participantComparator.setIgnoreStoichiometry(true);
            this.comparableParticipantsComparator = new CollectionComparator<>(participantComparator);

            File parsedFile = fileConfiguration.outputPath().toFile();
            assemblies = pdbAssembliesFileReader.readAssembliesFromParsedFile(parsedFile);

            proteinCacheMap = new HashMap<>();
        } catch (IOException e) {
            throw new ItemStreamException("Input file could not be read: " + fileConfiguration.outputPath(), e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            this.noChangesReportWriter.flush();
            this.complexesMissingAssembliesReportWriter.flush();
            this.complexesWithExtraAssembliesReportWriter.flush();
            this.complexesWithXrefsToUpdateReportWriter.flush();
            this.complexesWithXrefsToReviewReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.noChangesReportWriter.close();
            this.complexesMissingAssembliesReportWriter.close();
            this.complexesWithExtraAssembliesReportWriter.close();
            this.complexesWithXrefsToUpdateReportWriter.close();
            this.complexesWithXrefsToReviewReportWriter.close();
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    private void checkIfAssemblyMatchesAnyXref(
            Collection<Xref> xrefs,
            String assembly,
            Set<String> matchesFound,
            List<String> xrefsToAdd,
            List<InteractorXref> xrefsToUpdate,
            List<InteractorXref> xrefsToReview) {

        for (Xref xref: xrefs) {
            if (checkIfAssemblyMatchesXref(xref, assembly, matchesFound, xrefsToUpdate, xrefsToReview, true)) {
                return;
            }
        }
        for (Xref xref: xrefs) {
            if (checkIfAssemblyMatchesXref(xref, assembly, matchesFound, xrefsToUpdate, xrefsToReview, false)) {
                return;
            }
        }

        xrefsToAdd.add(assembly);
    }

    private boolean checkIfAssemblyMatchesXref(
            Xref xref,
            String assembly,
            Set<String> matchesFound,
            List<InteractorXref> xrefsToUpdate,
            List<InteractorXref> xrefsToReview,
            boolean caseSensitive) {

        InteractorXref interactorXref = (InteractorXref) xref;
        String xrefId = caseSensitive ? interactorXref.getId() : interactorXref.getId().toLowerCase();
        if (xrefId.equals(assembly)) {
            if (interactorXref.getQualifier() != null) {
                if (!Xref.IDENTITY_MI.equals(interactorXref.getQualifier().getMIIdentifier())) {
                    if (EXP_EVIDENCE.equals(interactorXref.getQualifier().getShortName())) {
                        xrefsToReview.add(interactorXref);
                    } else {
                        // Xref matches the PDB assembly, but it does not have qualifier set as identity or exp-evidence
                        xrefsToUpdate.add(interactorXref);
                    }
                }
            }
            matchesFound.add(interactorXref.getAc());
            return true;
        }
        return false;
    }

    private Collection<ModelledComparableParticipant> getProteinComponents(
            IntactComplex complex,
            Map<String, IntactProtein> proteinCacheMap) {

        return complex.getComparableParticipants(
                true,
                proteinAc -> {
                    if (!proteinCacheMap.containsKey(proteinAc)) {
                        IntactProtein protein = intactDao.getProteinDao().getByAc(proteinAc);
                        proteinCacheMap.put(proteinAc, protein);
                    }
                    return proteinCacheMap.get(proteinAc);
                }
        );
    }

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String sep = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.noChangesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "no_changes_needed" + extension), sep, header, ProcessReportWriter.NO_CHANGES_HEADER_LINE);
        this.complexesMissingAssembliesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "complexes_missing_xrefs" + extension), sep, header, ProcessReportWriter.COMPLEXES_WITH_ASSEMBLIES_TO_ADD);
        this.complexesWithExtraAssembliesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "complexes_with_extra_xrefs" + extension), sep, header, ProcessReportWriter.COMPLEXES_WITH_ASSEMBLIES_TO_REMOVE);
        this.complexesWithXrefsToUpdateReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "complexes_with_xrefs_to_update" + extension), sep, header, ProcessReportWriter.COMPLEXES_WITH_ASSEMBLIES_TO_UPDATE);
        this.complexesWithXrefsToReviewReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "complexes_with_xrefs_to_review" + extension), sep, header, ProcessReportWriter.COMPLEXES_WITH_XREFS_TO_REVIEW);
        this.ecoCodeChangesReportWriter = new ProcessReportWriter(
                new File(reportDirectory, "complexes_with_eco_codes_to_update" + extension), sep, header, ProcessReportWriter.ECO_CODE_CHANGE_HEADER_LINE);
        this.errorReportWriter = new ErrorsReportWriter(
                new File(reportDirectory, "process_errors" + extension), sep, header);
    }
}
