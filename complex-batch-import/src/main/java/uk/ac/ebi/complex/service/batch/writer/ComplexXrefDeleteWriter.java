package uk.ac.ebi.complex.service.batch.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.Complex;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.batch.logging.DeleteReportWriter;
import uk.ac.ebi.complex.service.batch.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.batch.model.ComplexWithXrefsToDelete;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Log4j
@SuperBuilder
public class ComplexXrefDeleteWriter extends AbstractBatchWriter<ComplexWithXrefsToDelete, Complex> {

    private DeleteReportWriter xrefsToDeleteReportWriter;
    private ErrorsReportWriter errorReportWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        try {
            this.xrefsToDeleteReportWriter.flush();
            this.errorReportWriter.flush();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be flushed", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        try {
            this.xrefsToDeleteReportWriter.close();
            this.errorReportWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be closed", e);
        }
    }

    @Override
    public void write(List<? extends ComplexWithXrefsToDelete> items) throws Exception {
        List<IntactComplex> complexesUpdated = new ArrayList<>();

        for (ComplexWithXrefsToDelete item: items) {
            List<Xref> identifiersToDelete = new ArrayList<>();
            List<Xref> xrefsToDelete = new ArrayList<>();

            IntactComplex complex = item.getComplex();
            String complexAc = complex.getComplexAc();
            for (Xref xref: item.getIdentityXrefs()) {
                if (appProperties.isDryRunMode()) {
                    xrefsToDeleteReportWriter.write(complexAc, xref.getId(), xref.getQualifier().getShortName());
                } else {
                    identifiersToDelete.add(xref);
                }
            }
            for (Xref xref: item.getSubsetXrefs()) {
                if (appProperties.isDryRunMode()) {
                    xrefsToDeleteReportWriter.write(complexAc, xref.getId(), xref.getQualifier().getShortName());
                } else {
                    xrefsToDelete.add(xref);
                }
            }
            for (Xref xref: item.getComplexClusterXrefs()) {
                if (appProperties.isDryRunMode()) {
                    xrefsToDeleteReportWriter.write(complexAc, xref.getId(), xref.getQualifier().getShortName());
                } else {
                    xrefsToDelete.add(xref);
                }
            }

            if (!appProperties.isDryRunMode()) {
                if (!identifiersToDelete.isEmpty() || !xrefsToDelete.isEmpty()) {
                    if (!identifiersToDelete.isEmpty()) {
                        complex.getIdentifiers().removeAll(identifiersToDelete);
                    }
                    if (!xrefsToDelete.isEmpty()) {
                        complex.getXrefs().removeAll(xrefsToDelete);
                    }
                    complexesUpdated.add(complex);
                }
            }
        }

        if (!appProperties.isDryRunMode()) {
            intactService.saveOrUpdate(complexesUpdated);
        }
    }

    @Override
    protected void initialiseReportWriters() throws IOException {
        super.initialiseReportWriters();

        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        this.xrefsToDeleteReportWriter = new DeleteReportWriter(
                new File(reportDirectory, "xrefs_to_delete" + extension), separator, header, DeleteReportWriter.DELETE_XREF_LINE);

        this.errorReportWriter = new ErrorsReportWriter(new File(reportDirectory, "delete_write_errors" + extension), separator, header);
    }
}
