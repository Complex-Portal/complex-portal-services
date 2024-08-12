package uk.ac.ebi.complex.service.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FileConfiguration {

    @Value("${input.file.name}")
    private String inputFileName;

    @Value("${output.directory}")
    private String reportDirectory = "reports";

    @Value("${output.file.name}")
    private String outputFileName = "output";

    @Value("${separator}")
    private String separator = "\t";

    @Value("${header}")
    private boolean header = true;

    public Path outputPath() {
        return Paths.get(this.reportDirectory, this.outputFileName + getExtension());
    }

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
