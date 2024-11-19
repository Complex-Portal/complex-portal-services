package uk.ac.ebi.complex.service.logging;

import uk.ac.ebi.complex.service.ComplexFinderResult;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

public class ProcessReportWriter<T> extends ReportWriter {

    public static final String[] NO_MATCH_HEADER_LINE = new String[]{
            "ids", "robustness", "uniprot_acs" };
    public static final String[] EXACT_MATCH_HEADER_LINE = new String[]{
            "match_type", "ids", "confidence", "uniprot_acs", "complex_ac", "complex_type" };
    public static final String[] MULTIPLE_EXACT_MATCHES_HEADER_LINE = new String[]{
            "match_type", "ids", "confidence", "uniprot_acs", "complex_acs", "complex_types" };
    public static final String[] PARTIAL_MATCHES_HEADER_LINE = new String[]{
            "match_type", "ids", "confidence", "uniprot_acs", "complex_ac", "complex_type",
            "num_proteins_in_common", "num_extra_proteins_in_complex", "num_proteins_missing_in_complex", "similarity" };

    public ProcessReportWriter(File outputFile, String separator, boolean header, String[] headerline) throws IOException {
        super(outputFile, separator, header, headerline);
    }

    public void write(ComplexFinderResult.MatchType matchType,
                      Collection<String> ids,
                      T confidence,
                      Collection<String> uniprotAcs,
                      String complexAc,
                      boolean predictedComplex,
                      int proteinsInCommon,
                      int extraProteinsInComplex,
                      int proteinsMissingInComplex,
                      double similarity) throws IOException {

        writeLine(new String[]{
                matchType != null ? matchType.name() : "",
                String.join(" ", ids),
                confidence.toString(),
                String.join(" ", uniprotAcs),
                complexAc,
                (predictedComplex ? "predicted" : "curated"),
                Integer.toString(proteinsInCommon),
                Integer.toString(extraProteinsInComplex),
                Integer.toString(proteinsMissingInComplex),
                Double.toString(similarity)
        });
        flush();
    }

    public void write(Collection<ComplexFinderResult.MatchType> matchTypes,
                      Collection<String> ids,
                      T confidence,
                      Collection<String> uniprotAcs,
                      Collection<String> complexAcs,
                      Collection<Boolean> predictedComplexes) throws IOException {

        writeLine(new String[]{
                matchTypes.stream().map(matchType -> matchType != null ? matchType.name() : "").collect(Collectors.joining(" ")),
                String.join(" ", ids),
                confidence.toString(),
                String.join(" ", uniprotAcs),
                String.join(" ", complexAcs),
                predictedComplexes.stream().map(predictedComplex -> predictedComplex ? "predicted" : "curated").collect(Collectors.joining(" "))
        });
        flush();
    }

    public void write(Collection<String> ids, T confidence, Collection<String> uniprotAcs) throws IOException {
        writeLine(new String[]{
                String.join(" ", ids),
                confidence.toString(),
                String.join(" ", uniprotAcs)
        });
        flush();
    }
}
