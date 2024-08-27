package uk.ac.ebi.complex.service.logging;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class WriteReportWriter extends ReportWriter {

    public WriteReportWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header, new String[]{ "cluster_ids", "cluster_confidence", "uniprot_acs", "complex_ac" });
    }

    public void write(Collection<String> clusterIds,
                      Integer clusterConfidence,
                      Collection<String> uniprotAcs,
                      String complexAc) throws IOException {

        csvWriter.writeNext(new String[]{
                String.join(" ", clusterIds),
                clusterConfidence.toString(),
                String.join(" ", uniprotAcs),
                complexAc
        });
        flush();
    }
}
