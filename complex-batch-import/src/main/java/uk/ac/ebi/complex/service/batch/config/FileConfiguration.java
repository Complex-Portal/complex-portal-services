package uk.ac.ebi.complex.service.batch.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
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

    public Path outputPath() {
        return Paths.get(this.getReportDirectory(), this.outputFileName + getExtension());
    }
}
