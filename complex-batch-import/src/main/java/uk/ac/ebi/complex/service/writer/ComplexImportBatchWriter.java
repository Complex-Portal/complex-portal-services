package uk.ac.ebi.complex.service.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.logging.WriteReportWriter;
import uk.ac.ebi.complex.service.manager.ComplexManager;
import uk.ac.ebi.complex.service.model.ComplexWithMatches;
import uk.ac.ebi.complex.service.model.ComplexToImport;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.model.lifecycle.LifeCycleStatus;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Log4j
@SuperBuilder
public class ComplexImportBatchWriter<T, R extends ComplexToImport<T>> extends AbstractBatchWriter<ComplexWithMatches<T, R>> {

    private final ComplexManager<T, R> complexManager;

    private WriteReportWriter<T> newComplexesReportWriter;
    private WriteReportWriter<T> updatedComplexesReportWriter;
    private WriteReportWriter<T> complexesToCreateReportWriter;
    private WriteReportWriter<T> complexesToUpdateReportWriter;
    private WriteReportWriter<T> complexesUnchangedReportWriter;
    private ErrorsReportWriter errorReportWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        try {
            this.newComplexesReportWriter.flush();
            this.updatedComplexesReportWriter.flush();
            this.complexesToCreateReportWriter.flush();
            this.complexesToUpdateReportWriter.flush();
            this.complexesUnchangedReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        try {
            this.newComplexesReportWriter.close();
            this.updatedComplexesReportWriter.close();
            this.complexesToCreateReportWriter.close();
            this.complexesToUpdateReportWriter.close();
            this.complexesUnchangedReportWriter.close();
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    @Override
    public void write(List<? extends ComplexWithMatches<T, R>> items) throws Exception {
        List<IntactComplex> newComplexes = new ArrayList<>();
        List<IntactComplex> updatedIdentityComplexes = new ArrayList<>();
        List<IntactComplex> updatedSubsetComplexes = new ArrayList<>();
        List<IntactComplex> updatedComplexClusterComplexes = new ArrayList<>();

        for (ComplexWithMatches<T, R> complexWithMatches: items) {
            R complexToImport = complexWithMatches.getComplexToImport();

            try {
                // Exact matches
                if (!complexWithMatches.getComplexesWithExactMatch().isEmpty()) {
                    for (IntactComplex existingComplex : complexWithMatches.getComplexesWithExactMatch()) {
                        boolean isComplexNotOnHold = LifeCycleStatus.RELEASED.equals(existingComplex.getStatus()) ||
                                LifeCycleStatus.READY_FOR_RELEASE.equals(existingComplex.getStatus());

                        if (isComplexNotOnHold && complexManager.doesComplexNeedUpdating(complexToImport, existingComplex)) {
                            updatedIdentityComplexes.add(complexManager.mergeComplexWithExistingComplex(complexToImport, existingComplex));
                            logComplexesToUpdate(complexToImport, List.of(existingComplex), Xref.IDENTITY);
                        } else {
                            logUnchangedComplexes(complexToImport, List.of(existingComplex), Xref.IDENTITY);
                        }
                    }
                } else {
                    newComplexes.add(complexManager.newComplex(complexToImport));
                    logNewComplexToCreate(complexToImport);
                }

                // Subset matches
                for (IntactComplex existingComplex : complexWithMatches.getComplexesToAddSubsetXref()) {
                    if (complexManager.doesComplexNeedSubsetXref(complexToImport, existingComplex)) {
                        updatedSubsetComplexes.add(complexManager.addSubsetXrefs(complexToImport, existingComplex));
                        logComplexesToUpdate(complexToImport, List.of(existingComplex), ComplexManager.SUBSET_QUALIFIER);
                    } else {
                        logUnchangedComplexes(complexToImport, List.of(existingComplex), ComplexManager.SUBSET_QUALIFIER);
                    }

                }

                // Complex cluster matches
                for (List<IntactComplex> complexCluster : complexWithMatches.getComplexesToAddComplexClusterXref()) {
                    boolean createComplexCluster = false;
                    for (IntactComplex existingComplex : complexCluster) {
                        if (complexManager.doesComplexNeedComplexClusterXref(complexToImport, existingComplex)) {
                            createComplexCluster = true;
                            updatedComplexClusterComplexes.add(complexManager.addComplexClusterXrefs(complexToImport, existingComplex));
                        }
                    }
                    if (createComplexCluster) {
                        logComplexesToUpdate(complexToImport, complexCluster, ComplexManager.COMPLEX_CLUSTER_QUALIFIER);
                    } else {
                        logUnchangedComplexes(complexToImport, complexCluster, ComplexManager.COMPLEX_CLUSTER_QUALIFIER);
                    }

                }
            } catch (Exception e) {
                log.error("Error writing to DB complexes: " + String.join(",", complexToImport.getComplexIds()), e);
                errorReportWriter.write(complexToImport.getComplexIds(), e.getMessage());
            }
        }

        if (!appProperties.isDryRunMode()) {
            this.complexService.saveOrUpdate(newComplexes);
            this.complexService.saveOrUpdate(updatedIdentityComplexes);
            this.complexService.saveOrUpdate(updatedSubsetComplexes);
            this.complexService.saveOrUpdate(updatedComplexClusterComplexes);

            for (ComplexWithMatches<T, R> complexWithMatches : items) {
                R complexToImport = complexWithMatches.getComplexToImport();
                loNewComplexes(complexToImport, newComplexes);
                logUpdatedComplexes(complexToImport, updatedIdentityComplexes, Xref.IDENTITY);
                logUpdatedComplexes(complexToImport, updatedSubsetComplexes, ComplexManager.SUBSET_QUALIFIER);
                logUpdatedComplexes(complexToImport, updatedComplexClusterComplexes, ComplexManager.COMPLEX_CLUSTER_QUALIFIER);
            }
        }
    }

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.newComplexesReportWriter = new WriteReportWriter<>(
                new File(reportDirectory, "new_complexes" + extension), separator, header, WriteReportWriter.UPDATE_COMPLEXES_LINE);
        this.updatedComplexesReportWriter = new WriteReportWriter<>(
                new File(reportDirectory, "updated_complexes" + extension), separator, header, WriteReportWriter.UPDATE_COMPLEXES_LINE);

        this.complexesToCreateReportWriter = new WriteReportWriter<>(
                new File(reportDirectory, "complexes_to_create" + extension), separator, header, WriteReportWriter.NEW_COMPLEXES_LINE);
        this.complexesToUpdateReportWriter = new WriteReportWriter<>(
                new File(reportDirectory, "complexes_to_update" + extension), separator, header, WriteReportWriter.UPDATE_COMPLEXES_LINE);
        this.complexesUnchangedReportWriter = new WriteReportWriter<>(
                new File(reportDirectory, "complexes_unchanged" + extension), separator, header, WriteReportWriter.UPDATE_COMPLEXES_LINE);

        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "write_errors" + extension), separator, header);
    }

    private void logNewComplexToCreate(R complex) throws IOException {
        complexesToCreateReportWriter.write(
                complex.getComplexIds(),
                complex.getConfidence(),
                complex.getProteinIds(),
                List.of(),
                Xref.IDENTITY);
    }

    private void logComplexesToUpdate(R complex, Collection<IntactComplex> existingComplexes, String qualifier) throws IOException {
        complexesToUpdateReportWriter.write(
                complex.getComplexIds(),
                complex.getConfidence(),
                complex.getProteinIds(),
                existingComplexes.stream().map(IntactComplex::getComplexAc).collect(Collectors.toList()),
                qualifier);
    }

    private void logUnchangedComplexes(R complex, Collection<IntactComplex> existingComplexes, String qualifier) throws IOException {
        complexesUnchangedReportWriter.write(
                complex.getComplexIds(),
                complex.getConfidence(),
                complex.getProteinIds(),
                existingComplexes.stream().map(IntactComplex::getComplexAc).collect(Collectors.toList()),
                qualifier);
    }

    private void loNewComplexes(R complex, Collection<IntactComplex> newComplexes) throws IOException {
        newComplexesReportWriter.write(
                complex.getComplexIds(),
                complex.getConfidence(),
                complex.getProteinIds(),
                newComplexes.stream().map(IntactComplex::getComplexAc).collect(Collectors.toList()),
                Xref.IDENTITY);
    }

    private void logUpdatedComplexes(R complex, Collection<IntactComplex> existingComplexes, String qualifier) throws IOException {
        updatedComplexesReportWriter.write(
                complex.getComplexIds(),
                complex.getConfidence(),
                complex.getProteinIds(),
                existingComplexes.stream().map(IntactComplex::getComplexAc).collect(Collectors.toList()),
                qualifier);
    }
}
