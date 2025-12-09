package uk.ac.ebi.complex.service.qsproteome.logging;

import uk.ac.ebi.complex.service.batch.logging.ReportWriter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class ProcessReportWriter extends ReportWriter {

    public static final String[] NO_MATCHES_HEADER_LINE = new String[]{
            "complex_id", "is_predicted" };
    public static final String[] COMPLEXES_WITH_QS_PROTEOME_STRUCTURES = new String[]{
            "complex_id", "is_predicted", "qs_proteome_structures" };

    public ProcessReportWriter(File outputFile, String separator, boolean header, String[] headerLine, boolean append) throws IOException {
        super(outputFile, separator, header, headerLine, append);
    }

    public void write(String complexId, boolean predicted) throws IOException {
        writeLine(new String[]{ complexId, String.valueOf(predicted) });
        flush();
    }

    public void write(String complexId,
                      boolean predicted,
                      Collection<String> structures) throws IOException {

        writeLine(new String[]{
                complexId,
                String.valueOf(predicted),
                String.join(" ", structures)
        });
        flush();
    }
}
