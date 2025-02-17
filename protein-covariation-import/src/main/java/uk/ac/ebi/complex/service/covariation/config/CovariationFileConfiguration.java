package uk.ac.ebi.complex.service.covariation.config;

import org.springframework.beans.factory.annotation.Value;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;

public class CovariationFileConfiguration extends FileConfiguration {

    @Value("${process.output.dir.name}")
    private String processOutputDirName;
}
