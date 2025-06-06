package uk.ac.ebi.complex.service.interactions.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class FileConfiguration {

    @Value("${output.directory}")
    private String reportDirectory = "reports";

    @Value("${separator}")
    private String separator = "\t";

    @Value("${header}")
    private boolean header = true;

    public String getSeparator() {
        return separator.replace("\\t", "\t");
    }

    public String getExtension() {
        if ("\t".equals(separator)) {
            return ".tsv";
        } else if (",".equals(separator)) {
            return ".csv";
        } else {
            return ".txt";
        }
    }
}
