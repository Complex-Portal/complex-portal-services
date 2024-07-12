package uk.ac.ebi.complex.service.logging;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import uk.ac.ebi.complex.service.ComplexFinderResult;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

public abstract class ReportWriter {

    protected final ICSVWriter csvWriter;

    public ReportWriter(File outputFile, String separator, boolean header) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        this.csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(separator.charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (header) {
            writeHeader();
        }
    }

    protected abstract void writeHeader();

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
        write(null, clusterIds, clusterConfidence, uniprotAcs, "");
    }

    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    public void close() throws IOException {
        this.csvWriter.close();
    }
}
