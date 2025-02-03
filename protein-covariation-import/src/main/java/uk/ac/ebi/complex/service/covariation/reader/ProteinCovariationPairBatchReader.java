package uk.ac.ebi.complex.service.covariation.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.covariation.model.ProteinPairCovariation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RequiredArgsConstructor
public class ProteinCovariationPairBatchReader implements ItemReader<List<ProteinPairCovariation>>, ItemStream {

    private static final int BATCH_SIZE = 25;

    private final FileConfiguration fileConfiguration;

    private CSVReader csvReader;
    private Iterator<File> fileIterator;
    private Iterator<String[]> csvIterator;

    @Override
    public List<ProteinPairCovariation> read() {
        List<ProteinPairCovariation> proteinCovariations = new ArrayList<>();
        while (hasNext() && proteinCovariations.size() < BATCH_SIZE) {
            String[] csvLine = csvIterator.next();
            proteinCovariations.add(proteinCovariationFromStringArray(csvLine));
        }
        if (!proteinCovariations.isEmpty()) {
            return proteinCovariations;
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        File inputDirectory = new File(fileConfiguration.getInputFileName());
        fileIterator = FileUtils.iterateFiles(inputDirectory, null, false);
        if (fileIterator.hasNext()) {
            loadNextFile();
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (csvReader != null) {
                csvReader.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    private boolean hasNext() {
        if (csvIterator != null && csvIterator.hasNext()) {
            return true;
        } else if (fileIterator.hasNext()) {
            loadNextFile();
            return hasNext();
        }
        return false;
    }

    private void loadNextFile() {
        File nextFile = fileIterator.next();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(nextFile));
            if (csvReader != null) {
                csvReader.close();
            }
            csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                    .build();

            if (fileConfiguration.isHeader()) {
                csvReader.skip(1);
            }

            csvIterator = csvReader.iterator();
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    private ProteinPairCovariation proteinCovariationFromStringArray(String[] csvLine) {
        String proteinA = csvLine[0];
        String proteinB = csvLine[1];
        String probability = csvLine[2];

        return ProteinPairCovariation.builder()
                .proteinA(proteinA)
                .proteinB(proteinB)
                .probability(Double.parseDouble(probability))
                .build();
    }
}
