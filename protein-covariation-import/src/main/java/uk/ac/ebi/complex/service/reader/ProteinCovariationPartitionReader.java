package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.Builder;
import lombok.extern.log4j.Log4j;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

@Log4j
@Builder
public class ProteinCovariationPartitionReader implements ItemReader<ProteinCovariation>, ItemStream {

    private final int partitionSize;
    private final int partitionIndex;
    private final int startLine;
    private final FileConfiguration fileConfiguration;

    private CSVReader csvReader;
    private Iterator<String[]> csvIterator;

    private int linesRead;

    @Override
    public ProteinCovariation read() {
        if (csvIterator.hasNext() && linesRead < partitionSize) {
            String[] csvLine = csvIterator.next();
            linesRead++;
            return proteinCovariationFromStringArray(csvLine);
        }
        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        Assert.notNull(executionContext, "ExecutionContext must not be null");

        linesRead = 0;

        log.info("Reading " + partitionSize + " covariations from partition " + partitionIndex + ", from line " + startLine);

        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileConfiguration.getInputFileName()));
            csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                    .build();

            if (startLine == 0) {
                if (fileConfiguration.isHeader()) {
                    csvReader.skip(1);
                }
            } else {
                csvReader.skip(startLine);
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
        if (csvReader != null) {
            try {
                csvReader.close();
                csvReader = null;
            } catch (IOException e) {
                throw new ItemStreamException(e);
            }
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
