package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;

public class DeleteReportWriter extends ReportWriter {

    public static final String[] DELETE_XREF_LINE = new String[]{
            "complex_ac", "xref", "qualifier" };

    public DeleteReportWriter(File outputFile, String separator, boolean header, String[] headerline) throws IOException {
        super(outputFile, separator, header, headerline);
    }

    public void write(String complexAc, String xrefId, String qualifier) throws IOException {
        writeLine(new String[]{ complexAc, xrefId, qualifier });
        flush();
    }
}
