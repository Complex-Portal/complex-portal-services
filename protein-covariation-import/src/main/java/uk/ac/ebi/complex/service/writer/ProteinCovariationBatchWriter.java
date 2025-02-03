package uk.ac.ebi.complex.service.writer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.ProteinPairCovariation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Log4j
@SuperBuilder
public class ProteinCovariationBatchWriter implements ItemWriter<List<ProteinPairCovariation>>, ItemStream {

    protected final FileConfiguration fileConfiguration;

    private ICSVWriter csvWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            initialiseReportWriters();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be opened", e);
        }

        File outputFile = fileConfiguration.outputPath().toFile();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
            csvWriter = new CSVWriterBuilder(writer)
                    .withSeparator(fileConfiguration.getSeparator().charAt(0))
                    .withQuoteChar(ICSVWriter.NO_QUOTE_CHARACTER)
                    .build();

            if (fileConfiguration.isHeader()) {
                csvWriter.writeNext(headerLine());
            }
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            csvWriter.close();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    @Override
    public void write(List<? extends List<ProteinPairCovariation>> items) throws Exception {
        items.forEach(proteinCovariations ->
                proteinCovariations.forEach(proteinCovariation ->
                        csvWriter.writeNext(proteinCovariationToStringArray(proteinCovariation))));
    }

    protected void initialiseReportWriters() throws IOException {
        File reportDirectory = new File(fileConfiguration.getReportDirectory());
        if (!reportDirectory.exists()) {
            reportDirectory.mkdirs();
        }
        if (!reportDirectory.isDirectory()) {
            throw new IOException("The reports directory has to be a directory: " + fileConfiguration.getReportDirectory());
        }
    }

    private String[] headerLine() {
        return new String[]{"protein_a", "protein_b", "confidence"};
    }

    private String[] proteinCovariationToStringArray(ProteinPairCovariation proteinCovariation) {
        String probability = proteinCovariation.getProbability().toString();
        return new String[]{
                proteinCovariation.getProteinA(),
                proteinCovariation.getProteinB(),
                probability
        };
    }
}
