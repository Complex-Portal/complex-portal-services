package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class WriteReportWriter extends ReportWriter {

    public static final String[] NEW_COMPLEXES_LINE = new String[]{
            "ids", "proteins", "xref_qualifier" };
    public static final String[] UPDATE_COMPLEXES_LINE = new String[]{
            "ids", "proteins", "complex_acs", "xref_qualifier" };

    public WriteReportWriter(File outputFile, String separator, boolean header, String[] headerline) throws IOException {
        super(outputFile, separator, header, headerline);
    }

    public void write(Collection<String> ids,
                      Collection<String> uniprotAcs,
                      Collection<String> complexAcs,
                      String xrefQualifier) throws IOException {

        writeLine(new String[]{
                String.join(" ", ids),
                String.join(" ", uniprotAcs),
                String.join(" ", complexAcs),
                xrefQualifier
        });
        flush();
    }
}
