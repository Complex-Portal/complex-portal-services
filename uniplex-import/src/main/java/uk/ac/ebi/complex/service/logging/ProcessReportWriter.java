package uk.ac.ebi.complex.service.logging;

import uk.ac.ebi.complex.service.ComplexFinderResult;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class ProcessReportWriter extends ReportWriter {

    public static final String[] NO_MATCH_HEADER_LINE = new String[]{
            "cluster_ids", "cluster_confidence", "uniprot_acs" };
    public static final String[] EXACT_MATCH_HEADER_LINE = new String[]{
            "match_type", "cluster_ids", "cluster_confidence", "uniprot_acs", "complex_ac" };
    public static final String[] PARTIAL_MATCHES_HEADER_LINE = new String[]{
            "match_type", "cluster_ids", "cluster_confidence", "uniprot_acs", "complex_ac",
            "num_proteins_in_common", "num_extra_proteins_in_complex", "num_proteins_missing_in_complex", "similarity" };

    public ProcessReportWriter(File outputFile, String separator, boolean header, String[] headerline) throws IOException {
        super(outputFile, separator, header, headerline);
    }

    public void write(ComplexFinderResult.MatchType matchType,
                      Collection<String> clusterIds,
                      Integer clusterConfidence,
                      Collection<String> uniprotAcs,
                      String complexAc,
                      int proteinsInCommon,
                      int extraProteinsInComplex,
                      int proteinsMissingInComplex,
                      double similarity) throws IOException {

        csvWriter.writeNext(new String[]{
                matchType != null ? matchType.name() : "",
                String.join(" ", clusterIds),
                clusterConfidence.toString(),
                String.join(" ", uniprotAcs),
                complexAc,
                Integer.toString(proteinsInCommon),
                Integer.toString(extraProteinsInComplex),
                Integer.toString(proteinsMissingInComplex),
                Double.toString(similarity)
        });
        flush();
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

    public void write(Collection<String> clusterIds, Integer clusterConfidence, Collection<String> uniprotAcs) throws IOException {
        csvWriter.writeNext(new String[]{
                String.join(" ", clusterIds),
                clusterConfidence.toString(),
                String.join(" ", uniprotAcs)
        });
        flush();
    }
}
