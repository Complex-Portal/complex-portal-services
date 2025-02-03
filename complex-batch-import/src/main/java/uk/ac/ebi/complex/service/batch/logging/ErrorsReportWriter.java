package uk.ac.ebi.complex.service.batch.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class ErrorsReportWriter extends ReportWriter {

    public ErrorsReportWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header, new String[]{ "ids", "error_message" });
    }

    public void write(Collection<String> ids, String errorMessage) throws IOException {
        writeLine(new String[]{ String.join(" ", ids), errorMessage });
        flush();
    }
}
