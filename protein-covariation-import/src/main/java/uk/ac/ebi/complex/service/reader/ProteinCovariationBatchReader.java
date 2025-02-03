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
import uk.ac.ebi.complex.service.model.ProteinCovariation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ProteinCovariationBatchReader implements ItemReader<List<ProteinCovariation>>, ItemStream {

    private static final int BATCH_SIZE = 50;

    private final FileConfiguration fileConfiguration;

    private CSVReader csvReader;
    private Iterator<String[]> csvIterator;

    @Override
    public List<ProteinCovariation> read() {
        List<ProteinCovariation> proteinCovariations = new ArrayList<>();
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

    private ProteinCovariation proteinCovariationFromStringArray(String[] csvLine) {
        String[] proteinAIds = csvLine[0].split(";");
        String[] proteinBIds = csvLine[1].split(";");
        String probability = csvLine[5];

        return ProteinCovariation.builder()
                .proteinA(Arrays.stream(proteinAIds)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()))
                .proteinB(Arrays.stream(proteinBIds)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()))
                .probability(Double.parseDouble(probability))
                .build();
    }
}
