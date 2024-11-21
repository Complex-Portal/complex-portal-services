package uk.ac.ebi.complex.service.reader;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

@SuperBuilder
public class MusicComplexWithNameReader extends ComplexFileReader<Double, MusicComplexToImport> {

    @Override
    protected MusicComplexToImport complexFromStringArray(String[] csvLine) {
        String complexName = csvLine[0];
        String complexId = csvLine[2];
        String[] uniprotAcs = csvLine[1].split(",");

        return MusicComplexToImport.builder()
                .name(complexName)
                .complexIds(Collections.singletonList(complexId))
                .proteinIds(Arrays.stream(uniprotAcs)
                        .map(String::trim)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()))
                .build();
    }
}
