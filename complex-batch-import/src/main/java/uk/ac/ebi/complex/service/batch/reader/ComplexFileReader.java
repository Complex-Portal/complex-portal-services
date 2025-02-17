package uk.ac.ebi.complex.service.batch.reader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.AllArgsConstructor;
import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.batch.model.ComplexToImport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@AllArgsConstructor
@SuperBuilder
public abstract class ComplexFileReader<T, R extends ComplexToImport<T>> {

    private final FileConfiguration fileConfiguration;

    public Collection<R> readComplexesFromFile(File inputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder().withSeparator(fileConfiguration.getSeparator().charAt(0)).build())
                .build();


        if (fileConfiguration.isHeader()) {
            csvReader.skip(1);
        }

        List<R> complexes = new ArrayList<>();
        csvReader.forEach(csvLine -> complexes.add(complexFromStringArray(csvLine)));
        csvReader.close();

        return complexes;
    }

    protected abstract R complexFromStringArray(String[] csvLine);
}
