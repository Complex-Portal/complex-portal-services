package uk.ac.ebi.complex.service.music.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class MusicImportAppProperties {

    @Value("${music.cell.line}")
    private String cellLine;

    @Value("${music.publication.id}")
    private String publicationId;

    @Value("${music.field.values.separator: }")
    private String fieldValuesSeparator;

    @Value("${music.input.file.fields}")
    private String inputFileFieldsString;

    private List<String> inputFileFields;

    private int getIndexOfField(String fieldName) {
        if (inputFileFields == null) {
            inputFileFields = Arrays.stream(inputFileFieldsString.split(",")).collect(Collectors.toList());
        }

        return inputFileFields.indexOf(fieldName);
    }

    public String[] fileFieldsArray() {
        return inputFileFieldsString.split(",");
    }

    public int idFieldIndex() {
        return getIndexOfField("ids");
    }

    public int proteinsFieldIndex() {
        return getIndexOfField("proteins");
    }

    public int confidenceFieldIndex() {
        return getIndexOfField("confidence");
    }

    public int nameFieldIndex() {
        return getIndexOfField("name");
    }

    public String getFieldValuesSeparator() {
        if (fieldValuesSeparator == null || fieldValuesSeparator.isEmpty()) {
            return " ";
        }
        return fieldValuesSeparator;
    }
}
