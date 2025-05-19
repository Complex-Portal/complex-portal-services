package uk.ac.ebi.complex.service.pdb.logging;

import uk.ac.ebi.complex.service.batch.logging.ReportWriter;

import java.io.File;
import java.io.IOException;

public class ErrorsReportWriter extends ReportWriter {

    public ErrorsReportWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header, new String[]{ "complex_id", "error_message" });
    }

    public void write(String complexId, String errorMessage) throws IOException {
        writeLine(new String[]{ complexId, errorMessage });
        flush();
    }
}
