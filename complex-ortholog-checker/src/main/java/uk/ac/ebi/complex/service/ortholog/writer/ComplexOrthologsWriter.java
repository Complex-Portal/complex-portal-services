package uk.ac.ebi.complex.service.ortholog.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.ortholog.config.FileConfiguration;
import uk.ac.ebi.complex.service.ortholog.logging.ReportWriter;
import uk.ac.ebi.complex.service.ortholog.model.ComplexOrthologs;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Log4j
@RequiredArgsConstructor
public class ComplexOrthologsWriter implements ItemWriter<ComplexOrthologs>, ItemStream {

    protected final FileConfiguration fileConfiguration;

    private ReportWriter complexesWithNoOrthologs;
    private ReportWriter complexesWithOrthologs;

    @Override
    public void write(List<? extends ComplexOrthologs> items) {
        for (ComplexOrthologs item : items) {
            if (item.getOutputComplexIds().isEmpty()) {
                complexesWithNoOrthologs.writeLine(
                        new String[]{ item.getInputComplexId() });
            } else {
                complexesWithOrthologs.writeLine(
                        new String[]{ item.getInputComplexId(), String.join("|", item.getOutputComplexIds()) });
            }
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            initialiseReportWriters();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be opened", e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {
    }

    protected void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + fileConfiguration.getReportDirectory());
        }

        String separator = fileConfiguration.getSeparator();
        boolean header = fileConfiguration.isHeader();
        String extension = fileConfiguration.getExtension();

        File noOrthologsFile = new File(reportDirectory, "no_orthologs" + extension);
        this.complexesWithNoOrthologs = new ReportWriter(noOrthologsFile, separator, header, new String[]{"complex_id"});
        File withOrthologsFile = new File(reportDirectory, "with_orthologs" + extension);
        this.complexesWithOrthologs = new ReportWriter(withOrthologsFile, separator, header, new String[]{"complex_id", "complex_orthologs"});
    }
}
