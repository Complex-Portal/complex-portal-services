package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class ErrorsReportWriter extends ReportWriter {

    public ErrorsReportWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header);
    }

    protected void writeHeader() {
        csvWriter.writeNext(new String[]{ "cluster_ids", "error_message" });
    }

    public void write(Collection<String> clusterIds, String errorMessage) throws IOException {
        csvWriter.writeNext(new String[]{ String.join(" ", clusterIds), errorMessage });
        flush();
    }
}
