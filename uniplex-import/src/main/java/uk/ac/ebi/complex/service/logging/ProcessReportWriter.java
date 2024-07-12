package uk.ac.ebi.complex.service.logging;

import uk.ac.ebi.complex.service.ComplexFinderResult;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class ProcessReportWriter extends ReportWriter {

    public ProcessReportWriter(File outputFile, String separator, boolean header) throws IOException {
        super(outputFile, separator, header);
    }

    protected void writeHeader() {
        csvWriter.writeNext(new String[]{ "match_type", "cluster_ids", "cluster_confidence", "uniprot_acs", "complex_ac" });
    }

    public void write(ComplexFinderResult.MatchType matchType,
                      Collection<String> clusterIds,
                      Integer clusterConfidence,
                      Collection<String> uniprotAcs,
                      String complexAc) throws IOException {

        csvWriter.writeNext(new String[]{
                matchType != null ? matchType.name() : "",
                String.join(" ", clusterIds),
                clusterConfidence.toString(),
                String.join(" ", uniprotAcs),
                complexAc
        });
        flush();
    }
}
