package uk.ac.ebi.complex.service.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
public class AppProperties {

    @Value("${db.hbm2ddl}")
    private String hbm2ddl;

    @Value("${db.dialect}")
    private String dbDialect;

    @Value("${db.driver}")
    private String dbDriver;

    @Value("${db.url}")
    private String dbUrl;

    @Value("${db.user}")
    private String dbUsername;

    @Value("${db.password}")
    private String dbPassword;

    @Value("${jami.user.context.id}")
    private String userContextId;

    @Value("${jami.ac.prefix}")
    private String acPrefix;
}
