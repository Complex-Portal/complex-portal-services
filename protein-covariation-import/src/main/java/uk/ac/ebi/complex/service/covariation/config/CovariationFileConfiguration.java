package uk.ac.ebi.complex.service.covariation.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;

@Configuration
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class CovariationFileConfiguration extends FileConfiguration {

    @Value("${process.output.dir.name}")
    private String processOutputDirName;
}
