package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import uk.ac.ebi.complex.service.config.FileConfiguration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
@RequiredArgsConstructor
public class ProteinIdsReader {

    private final FileConfiguration fileConfiguration;

    public Collection<List<String>> readProteinIdsFromFile() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileConfiguration.getInputFileName()));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();

        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        long count = 0L;

        Map<String, List<String>> proteinIds = new HashMap<>();
        for (String[] csvLine : csvReader) {
            proteinIdsFromStringArray(proteinIds, csvLine);
            count++;
            if (count % 2_500_000 == 0) {
                log.info("Read " + count + " proteinIds");
            }
        }
        csvReader.close();

        return proteinIds.values();
    }

    private void proteinIdsFromStringArray(Map<String, List<String>> proteinIds, String[] csvLine) {
        String proteinsA = csvLine[0];
        String proteinsB = csvLine[1];
        if (!proteinIds.containsKey(proteinsA)) {
            proteinIds.put(proteinsA, Arrays.asList(proteinsA.split(";")));
        }
        if (!proteinIds.containsKey(proteinsB)) {
            proteinIds.put(proteinsB, Arrays.asList(proteinsB.split(";")));
        }
    }
}
