package uk.ac.ebi.complex.service.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import psidev.psi.mi.jami.model.Xref;
import psidev.psi.mi.jami.utils.XrefUtils;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.logging.ProcessReportWriter;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblies;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblyXrefs;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.extension.InteractorXref;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j
@Component
@RequiredArgsConstructor
public class PdbAssembliesProcessor extends AbstractBatchProcessor<ComplexWithAssemblies, ComplexWithAssemblyXrefs> {

    private static final String WWPDB_DB_MI = "MI:0805";
    private static final String WWPDB_DB_NAME = "wwpdb";
    private static final String EXP_EVIDENCE = "exp-evidence";

    private final IntactDao intactDao;
    private final FileConfiguration fileConfiguration;

    private ProcessReportWriter noChangesReportWriter;
    private ProcessReportWriter complexesMissingAssembliesReportWriter;
    private ProcessReportWriter complexesWithExtraAssembliesReportWriter;
    private ProcessReportWriter complexesWithXrefsToUpdateReportWriter;
    private ProcessReportWriter complexesWithXrefsToReviewReportWriter;
    private ErrorsReportWriter errorReportWriter;

    @Override
    public ComplexWithAssemblyXrefs process(ComplexWithAssemblies item) throws Exception {
        try {
            IntactComplex complex = intactDao.getComplexDao().getLatestComplexVersionByComplexAc(item.getComplexId());
            Collection<Xref> pdbIdentifiers = XrefUtils.collectAllXrefsHavingDatabase(complex.getIdentifiers(), WWPDB_DB_MI, WWPDB_DB_NAME);
            Collection<Xref> pdbXrefs = XrefUtils.collectAllXrefsHavingDatabase(complex.getXrefs(), WWPDB_DB_MI, WWPDB_DB_NAME);

            List<String> xrefsToAdd = new ArrayList<>();
            List<InteractorXref> xrefsToRemove = new ArrayList<>();
            List<InteractorXref> xrefsToUpdate = new ArrayList<>();
            List<InteractorXref> xrefsToReview = new ArrayList<>();

            if (item.getAssemblies().isEmpty()) {
                // No PDB assemblies for this complex, we delete all PDB xrefs with identity qualifier
                pdbIdentifiers.stream()
                        .filter(xref -> xref.getQualifier() != null && Xref.IDENTITY_MI.equals(xref.getQualifier().getMIIdentifier()))
                        .forEach(xref -> xrefsToRemove.add((InteractorXref) xref));
                pdbXrefs.stream()
                        .filter(xref -> xref.getQualifier() != null && Xref.IDENTITY_MI.equals(xref.getQualifier().getMIIdentifier()))
                        .forEach(xref -> xrefsToRemove.add((InteractorXref) xref));
                pdbIdentifiers.stream()
                        .filter(xref -> xref.getQualifier() != null && EXP_EVIDENCE.equals(xref.getQualifier().getShortName()))
                        .forEach(xref -> xrefsToReview.add((InteractorXref) xref));
                pdbXrefs.stream()
                        .filter(xref -> xref.getQualifier() != null && EXP_EVIDENCE.equals(xref.getQualifier().getShortName()))
                        .forEach(xref -> xrefsToReview.add((InteractorXref) xref));
            } else {
                Set<String> matchesFound = new HashSet<>();

                pdbIdentifiers.forEach(xref -> checkIfXrefNeedsUpdateOrRemove(
                        xref, item.getAssemblies(), matchesFound, xrefsToRemove, xrefsToUpdate, xrefsToReview));
                pdbXrefs.forEach(xref -> checkIfXrefNeedsUpdateOrRemove(
                        xref, item.getAssemblies(), matchesFound, xrefsToRemove, xrefsToUpdate, xrefsToReview));

                for (String assembly: item.getAssemblies()) {
                    if (!matchesFound.contains(assembly)) {
                        xrefsToAdd.add(assembly);
                    }
                }
            }

            if (xrefsToAdd.isEmpty() && xrefsToRemove.isEmpty() && xrefsToUpdate.isEmpty() && xrefsToReview.isEmpty()) {
                noChangesReportWriter.write(item.getComplexId(), item.getAssemblies());
            } else if (!xrefsToAdd.isEmpty() && xrefsToRemove.isEmpty() && xrefsToUpdate.isEmpty() && xrefsToReview.isEmpty()) {
                complexesMissingAssembliesReportWriter.write(
                        item.getComplexId(),
                        item.getAssemblies(),
                        xrefsToAdd);
            } else if (!xrefsToRemove.isEmpty() && xrefsToAdd.isEmpty() && xrefsToUpdate.isEmpty() && xrefsToReview.isEmpty()) {
                complexesWithExtraAssembliesReportWriter.write(
                        item.getComplexId(),
                        item.getAssemblies(),
                        xrefsToRemove.stream().map(InteractorXref::getId).collect(Collectors.toList()));
            } else if (!xrefsToUpdate.isEmpty() && xrefsToAdd.isEmpty() && xrefsToRemove.isEmpty() && xrefsToReview.isEmpty()) {
                complexesWithXrefsToUpdateReportWriter.write(
                        item.getComplexId(),
                        item.getAssemblies(),
                        xrefsToUpdate.stream().map(InteractorXref::getId).collect(Collectors.toList()));
            }  else {
                complexesWithXrefsToReviewReportWriter.write(
                        item.getComplexId(),
                        item.getAssemblies(),
                        xrefsToAdd,
                        xrefsToRemove.stream().map(InteractorXref::getId).collect(Collectors.toList()),
                        xrefsToUpdate.stream()
                                .map(xref -> xref.getId() + "(" + (xref.getQualifier() != null ? xref.getQualifier().getShortName() : "" ) + ")")
                                .collect(Collectors.toList()),
                        xrefsToReview.stream()
                                .map(xref -> xref.getId() + "(" + (xref.getQualifier() != null ? xref.getQualifier().getShortName() : "" ) + ")")
                                .collect(Collectors.toList()));
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

    private void checkIfXrefNeedsUpdateOrRemove(
            Xref xref,
            Collection<String> assemblies,
            Set<String> matchesFound,
            List<InteractorXref> xrefsToRemove,
            List<InteractorXref> xrefsToUpdate,
            List<InteractorXref> xrefsToReview) {

        String xrefId = xref.getId().toLowerCase();

        for (String assembly: assemblies) {
            if (!matchesFound.contains(assembly)) {
                if (xrefId.equals(assembly)) {
                    if (xref.getQualifier() == null) {
                        if (!Xref.IDENTITY_MI.equals(xref.getQualifier().getMIIdentifier())) {
                            if (EXP_EVIDENCE.equals(xref.getQualifier().getShortName())) {
                                xrefsToReview.add((InteractorXref) xref);
                            } else {
                                // Xref matches the PDB assembly, but it does not have qualifier set as identity or exp-evidence
                                xrefsToUpdate.add((InteractorXref) xref);
                            }
                        }
                    }
                    matchesFound.add(assembly);
                    return;
                }
            }
        }

        if (xref.getQualifier() != null) {
            if (Xref.IDENTITY_MI.equals(xref.getQualifier().getMIIdentifier())) {
                // Xref does not match any PDB assembly, we delete it as it has identity qualifier
                xrefsToRemove.add((InteractorXref) xref);
            } else if (EXP_EVIDENCE.equals(xref.getQualifier().getShortName())) {
                xrefsToReview.add((InteractorXref) xref);
            }
        }
    }

    @Override
    protected FileConfiguration getFileConfiguration() {
        return fileConfiguration;
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
        this.errorReportWriter = new ErrorsReportWriter(
                new File(reportDirectory, "process_errors" + extension), sep, header);
    }
}
