package uk.ac.ebi.complex.service.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ComplexServiceConfiguration {
    private String inputFileName;
    private String reportDirectory = "reports";
    private String outputFileName = "output.tsv";
    private String separator = "\t";
    private boolean header = true;

    public Path outputPath() {
        return Paths.get(this.reportDirectory, this.outputFileName);
    }

    public void setSeparator(String separator) {
        System.out.println(separator);
        this.separator = separator.replace("\\t", "\t");
    }

    public static class ComplexServiceConfigurationBuilder {
        public ComplexServiceConfigurationBuilder separator(String separator) {
            System.out.println(separator);
            this.separator = separator.replace("\\t", "\t");
            return this;
        }

    }

}
