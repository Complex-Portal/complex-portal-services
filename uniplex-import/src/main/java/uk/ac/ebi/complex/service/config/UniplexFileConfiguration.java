package uk.ac.ebi.complex.service.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.nio.file.Paths;

@EqualsAndHashCode(callSuper = true)
@Configuration
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public class UniplexFileConfiguration extends FileConfiguration {

    @Value("${output.file.name}")
    private String outputFileName = "output";

    public Path outputPath() {
        return Paths.get(this.getReportDirectory(), this.outputFileName + getExtension());
    }
}
