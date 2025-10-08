package uk.ac.ebi.complex.service.batch.logging;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class ReportWriter {

    private final ICSVWriter csvWriter;

    public ReportWriter(File outputFile, String separator, boolean header, String[] headerLine) throws IOException {
        this(outputFile, separator, header, headerLine, false);
    }

    public ReportWriter(File outputFile, String separator, boolean header, String[] headerLine, boolean append) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, append));
        this.csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(separator.charAt(0))
                .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                .build();

        if (header && !append) {
            writeLine(headerLine);
        }
    }

    protected void writeLine(String[] line) {
        csvWriter.writeNext(line);
    }

    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    public void close() throws IOException {
        this.csvWriter.close();
    }
}
