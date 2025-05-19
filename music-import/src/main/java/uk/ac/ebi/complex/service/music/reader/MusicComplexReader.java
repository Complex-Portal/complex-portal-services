package uk.ac.ebi.complex.service.music.reader;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.batch.reader.ComplexFileReader;
import uk.ac.ebi.complex.service.music.config.MusicImportAppProperties;
import uk.ac.ebi.complex.service.music.model.MusicComplexToImport;

import java.util.Arrays;
import java.util.stream.Collectors;

@SuperBuilder
public class MusicComplexReader extends ComplexFileReader<Double, MusicComplexToImport> {

    private final MusicImportAppProperties musicImportAppProperties;

    @Override
    protected MusicComplexToImport complexFromStringArray(String[] csvLine) {
        MusicComplexToImport complex = new MusicComplexToImport();

        String idsString = csvLine[musicImportAppProperties.idFieldIndex()];
        String[] ids = idsString.split(musicImportAppProperties.getFieldValuesSeparator());
        complex.setComplexIds(Arrays.stream(ids)
                .map(String::trim)
                .sorted()
                .distinct()
                .collect(Collectors.toList()));

        String proteinsString = csvLine[musicImportAppProperties.proteinsFieldIndex()];
        String[] proteins = proteinsString.split(musicImportAppProperties.getFieldValuesSeparator());
        complex.setProteinIds(Arrays.stream(proteins)
                .map(String::trim)
                .sorted()
                .distinct()
                .collect(Collectors.toList()));

        if (musicImportAppProperties.confidenceFieldIndex() > -1) {
            String confidence = csvLine[musicImportAppProperties.confidenceFieldIndex()];
            complex.setConfidence(Double.parseDouble(confidence.trim()));
        }

        if (musicImportAppProperties.nameFieldIndex() > -1) {
            String name = csvLine[musicImportAppProperties.nameFieldIndex()];
            complex.setName(name.trim());
        }

        return complex;
    }
}
