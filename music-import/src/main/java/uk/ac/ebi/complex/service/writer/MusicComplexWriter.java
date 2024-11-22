package uk.ac.ebi.complex.service.writer;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.config.MusicImportAppProperties;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
public class MusicComplexWriter extends ComplexFileWriter<Double, MusicComplexToImport> {

    private final MusicImportAppProperties musicImportAppProperties;

    @Override
    protected String[] headerLine() {
        return musicImportAppProperties.fileFieldsArray();
    }

    @Override
    protected String[] complexToStringArray(MusicComplexToImport complex) {
        Map<Integer, String> fieldsWithIndexes = new HashMap<>();

        String ids = String.join(musicImportAppProperties.getFieldValuesSeparator(), complex.getComplexIds());
        fieldsWithIndexes.put(musicImportAppProperties.idFieldIndex(), ids);

        String proteinIds = String.join(musicImportAppProperties.getFieldValuesSeparator(), complex.getProteinIds());
        fieldsWithIndexes.put(musicImportAppProperties.proteinsFieldIndex(), proteinIds);

        if (musicImportAppProperties.nameFieldIndex() > -1) {
            fieldsWithIndexes.put(musicImportAppProperties.nameFieldIndex(), complex.getName());
        }

        if (musicImportAppProperties.confidenceFieldIndex() > -1) {
            String confidence = complex.getConfidence().toString();
            fieldsWithIndexes.put(musicImportAppProperties.confidenceFieldIndex(), confidence);
        }
        String[] values = fieldsWithIndexes.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toArray(String[]::new);

        return values;
    }
}
