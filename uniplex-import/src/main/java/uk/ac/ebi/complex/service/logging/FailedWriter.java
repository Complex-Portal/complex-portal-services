package uk.ac.ebi.complex.service.logging;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

public class FailedWriter {

    private final ICSVWriter csvWriter;

    public FailedWriter(File outputFile, String separator, boolean header) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        this.csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(separator.charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (header) {
            writeHeader();
        }
    }

    private void writeHeader() {
        csvWriter.writeNext(new String[]{"cluster_id", "participant_acs","problematic_acs", "reasons"});
    }

    public void write(Collection<String> clusterIds, Collection<String> participantIds, Collection<String> problematicAcs, Collection<String> reasons) throws IOException {
        csvWriter.writeNext(new String[]{
                String.join(" ", clusterIds),
                String.join(" ", participantIds),
                String.join(" ", problematicAcs),
                String.join("; ", reasons)
        });
        flush();
    }

    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    public void close() throws IOException {
        this.csvWriter.close();
    }
}
