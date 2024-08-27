package uk.ac.ebi.complex.service.logging;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class ReportWriter {

    protected final ICSVWriter csvWriter;

    public ReportWriter(File outputFile, String separator, boolean header, String[] headerLine) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        this.csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(separator.charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (header) {
            writeHeader(headerLine);
        }
    }

    private void writeHeader(String[] headerLine) {
        csvWriter.writeNext(headerLine);
    }

    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    public void close() throws IOException {
        this.csvWriter.close();
    }
}
