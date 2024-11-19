package uk.ac.ebi.complex.service.writer;

import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import psidev.psi.mi.jami.model.Xref;
import uk.ac.ebi.complex.service.logging.DeleteReportWriter;
import uk.ac.ebi.complex.service.logging.ErrorsReportWriter;
import uk.ac.ebi.complex.service.model.ComplexWithXrefsToDelete;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Log4j
@SuperBuilder
public class ComplexXrefDeleteWriter extends AbstractBatchWriter<ComplexWithXrefsToDelete> {

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
        for (ComplexWithXrefsToDelete item: items) {
            String complexAc = item.getComplex().getComplexAc();
            for (Xref xref: item.getIdentityXrefs()) {
                xrefsToDeleteReportWriter.write(complexAc, xref.getId(), xref.getQualifier().getShortName());
            }
            for (Xref xref: item.getSubsetXrefs()) {
                xrefsToDeleteReportWriter.write(complexAc, xref.getId(), xref.getQualifier().getShortName());
            }
            for (Xref xref: item.getComplexClusterXrefs()) {
                xrefsToDeleteReportWriter.write(complexAc, xref.getId(), xref.getQualifier().getShortName());
            }
        }

        // TODO: actual call to DB to delete xrefs
        if (!appProperties.isDryRunMode()) {
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
