package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;

public class ProteinFailedWriter extends ReportWriter {

    public ProteinFailedWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header, new String[]{ "problematic_ac", "reasons" });
    }

    public void write(String proteinAc, String reason) throws IOException {
        writeLine(new String[]{ proteinAc, reason });
        flush();
    }
}
