package uk.ac.ebi.complex.service.reader;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;

import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
public class MusicComplexWithConfidenceReader extends ComplexFileReader<Double, MusicComplexToImport> {

    @Override
    protected MusicComplexToImport complexFromStringArray(String[] csvLine) {
            String complexId = csvLine[0];
            String[] uniprotAcs = csvLine[1].split(" ");
            String robustness = csvLine[2];

        return MusicComplexToImport.builder()
                .complexIds(Collections.singletonList(complexId))
                .confidence(Double.parseDouble(robustness))
                .proteinIds(Arrays.stream(uniprotAcs)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList()))
                .build();
    }
}
