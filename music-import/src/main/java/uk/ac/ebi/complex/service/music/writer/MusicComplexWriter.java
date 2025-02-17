package uk.ac.ebi.complex.service.music.writer;

import lombok.experimental.SuperBuilder;
import uk.ac.ebi.complex.service.batch.writer.ComplexFileWriter;
import uk.ac.ebi.complex.service.music.config.MusicImportAppProperties;
import uk.ac.ebi.complex.service.music.model.MusicComplexToImport;

import java.util.HashMap;
import java.util.Map;

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
