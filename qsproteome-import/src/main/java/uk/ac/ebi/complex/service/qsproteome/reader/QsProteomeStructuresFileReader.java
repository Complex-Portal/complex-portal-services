package uk.ac.ebi.complex.service.qsproteome.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Log4j
@Component
@RequiredArgsConstructor
public class QsProteomeStructuresFileReader {

    private final FileConfiguration fileConfiguration;

    public Map<String, Collection<String>> readStructuresFromFile(File inputFile) throws IOException {
        Map<String, Collection<String>> structuresByComplex = new HashMap<>();

        if (inputFile.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                    .build();

            if (fileConfiguration.isHeader()) {
                csvReader.skip(1);
            }

            csvReader.forEach(csvLine -> {
                if (csvLine.length == 3) {
                    String complexId = csvLine[0];
                    List<String> structures = List.of(csvLine[2].split(" ")[0]);
                    structuresByComplex.put(complexId, structures);
                }
            });
            csvReader.close();
        }

        return structuresByComplex;
    }

    public Set<String> readComplexIdsFromFile(File inputFile) throws IOException {
        Set<String> complexIds = new HashSet<>();

        if (inputFile.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                    .build();

            if (fileConfiguration.isHeader()) {
                csvReader.skip(1);
            }

            csvReader.forEach(csvLine -> {
                if (csvLine.length > 0) {
                    String complexId = csvLine[0];
                    complexIds.add(complexId);
                }
            });
            csvReader.close();
        }

        return complexIds;
    }
}
