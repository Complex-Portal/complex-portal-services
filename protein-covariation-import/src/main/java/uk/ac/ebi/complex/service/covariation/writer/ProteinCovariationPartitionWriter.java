package uk.ac.ebi.complex.service.covariation.writer;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.covariation.config.CovariationFileConfiguration;
import uk.ac.ebi.complex.service.covariation.model.ProteinPairCovariation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Log4j
@SuperBuilder
public class ProteinCovariationPartitionWriter implements ItemWriter<List<ProteinPairCovariation>>, ItemStream {

    private final int partitionIndex;
    protected final CovariationFileConfiguration fileConfiguration;

    private ICSVWriter csvWriter;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
        try {
            initialiseReportWriters();
        } catch (IOException e) {
            throw new ItemStreamException("Report file writer could not be opened", e);
        }

        File outputDirectory = Paths.get(fileConfiguration.getReportDirectory(), fileConfiguration.getProcessOutputDirName()).toFile();
        if (!outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }
        if (!outputDirectory.isDirectory()) {
            throw new ItemStreamException("The output directory has to be a directory: " + outputDirectory.toPath());
        }

        File outputFile = new File(outputDirectory, partitionIndex + fileConfiguration.getExtension());

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
        if (csvWriter != null) {
            try {
                csvWriter.close();
                csvWriter = null;
            } catch (IOException e) {
                throw new ItemStreamException(e);
            }
        }
    }

    @Override
    public void write(List<? extends List<ProteinPairCovariation>> items) throws Exception {
        items.forEach(proteinCovariations ->
                proteinCovariations.forEach(proteinCovariation ->
                        csvWriter.writeNext(proteinCovariationToStringArray(proteinCovariation))));
        csvWriter.flush();
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
        return new String[]{"protein_a_ac", "protein_b_ac", "confidence"};
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
