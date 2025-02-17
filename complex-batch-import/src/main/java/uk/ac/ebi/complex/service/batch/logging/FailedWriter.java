package uk.ac.ebi.complex.service.batch.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class FailedWriter extends ReportWriter {

    public FailedWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header, new String[]{ "id", "participant_acs","problematic_acs", "reasons" });
    }

    public void write(
            Collection<String> ids,
            Collection<String> participantIds,
            Collection<String> problematicAcs,
            Collection<String> reasons) throws IOException {

        writeLine(new String[]{
                String.join(" ", ids),
                String.join(" ", participantIds),
                String.join(" ", problematicAcs),
                String.join("; ", reasons)
        });
        flush();
    }
}
