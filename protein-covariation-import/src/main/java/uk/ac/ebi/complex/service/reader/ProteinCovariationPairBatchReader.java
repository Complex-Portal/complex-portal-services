package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;
import uk.ac.ebi.complex.service.config.FileConfiguration;
import uk.ac.ebi.complex.service.model.ProteinPairCovariation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RequiredArgsConstructor
public class ProteinCovariationPairBatchReader implements ItemReader<List<ProteinPairCovariation>>, ItemStream {

    private static final int BATCH_SIZE = 10;

    private final FileConfiguration fileConfiguration;

    private CSVReader csvReader;
    private Iterator<String[]> csvIterator;

    @Override
    public List<ProteinPairCovariation> read() {
        List<ProteinPairCovariation> proteinCovariations = new ArrayList<>();
        while (csvIterator.hasNext() && proteinCovariations.size() < BATCH_SIZE) {
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

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(fileConfiguration.getInputFileName()));
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

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            csvReader.close();
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
