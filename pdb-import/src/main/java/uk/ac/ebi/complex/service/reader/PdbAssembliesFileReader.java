package uk.ac.ebi.complex.service.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import uk.ac.ebi.complex.service.config.FileConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Log4j
@Component
@RequiredArgsConstructor
public class PdbAssembliesFileReader {

    private final FileConfiguration fileConfiguration;

    public Map<String, Set<String>> readAssembliesFromFile(File inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();


        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        Map<String, Set<String>> complexAssemblies = new HashMap<>();
        csvReader.forEach(csvLine -> {
            String complexId = csvLine[4];
            if (StringUtils.isNotEmpty(complexId)) {
            String assembly = csvLine[7].split("_")[0];
                complexAssemblies.putIfAbsent(complexId, new HashSet<>());
                complexAssemblies.get(complexId).add(assembly);
            }
        });
        csvReader.close();

        return complexAssemblies;
    }
}
