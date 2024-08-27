package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class FailedWriter extends ReportWriter {

    public FailedWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header, new String[]{ "cluster_id", "participant_acs","problematic_acs", "reasons" });
    }

    public void write(
            Collection<String> clusterIds,
            Collection<String> participantIds,
            Collection<String> problematicAcs,
            Collection<String> reasons) throws IOException {

        csvWriter.writeNext(new String[]{
                String.join(" ", clusterIds),
                String.join(" ", participantIds),
                String.join(" ", problematicAcs),
                String.join("; ", reasons)
        });
        flush();
    }
}
