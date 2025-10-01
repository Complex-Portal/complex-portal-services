package uk.ac.ebi.complex.service.qsproteome.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;
import psidev.psi.mi.jami.batch.BasicChunkLoggerListener;
import psidev.psi.mi.jami.batch.SimpleJobListener;
import uk.ac.ebi.complex.service.batch.config.AppProperties;
import uk.ac.ebi.complex.service.batch.config.FileConfiguration;
import uk.ac.ebi.complex.service.qsproteome.model.ComplexWithProteomeStructures;
import uk.ac.ebi.complex.service.qsproteome.processor.ComplexQsProteomeProcessor;
import uk.ac.ebi.complex.service.qsproteome.reader.ComplexReader;
import uk.ac.ebi.complex.service.qsproteome.writer.ComplexQsProteomeWriter;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;

import java.net.http.HttpClient;

@Configuration
public class QsProteomeImportConfig {

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("/META-INF/qsproteome-import.properties"));
        return configurer;
    }

    @Bean
    public ComplexQsProteomeProcessor complexQsProteomeProcessor(IntactDao intactDao) {
        return ComplexQsProteomeProcessor.builder()
                .intactDao(intactDao)
                .client(HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build())
                .mapper(new ObjectMapper())
                .rateLimiter(RateLimiter.create(2.0))
                .build();
    }

    @Bean
    public ComplexQsProteomeWriter complexQsProteomeWriter(
            IntactDao intactDao,
            AppProperties appProperties,
            FileConfiguration fileConfiguration,
            ComplexService complexService) {

        return ComplexQsProteomeWriter.builder()
                .intactDao(intactDao)
                .appProperties(appProperties)
                .fileConfiguration(fileConfiguration)
                .intactService(complexService)
                .build();

    }

    @Bean
    public Step complexQsProteomeImportStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ComplexReader complexReader,
            ComplexQsProteomeProcessor complexQsProteomeProcessor,
            ComplexQsProteomeWriter complexQsProteomeWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "complexQsProteomeImportStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<IntactComplex, ComplexWithProteomeStructures>(basicStep)
                .chunk(50)
                .reader(complexReader)
                .processor(complexQsProteomeProcessor)
                .writer(complexQsProteomeWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    public Job complexQsProteomeImportJob(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobListener basicJobLoggerListener,
            @Qualifier("complexQsProteomeImportStep") Step importStep) throws Exception {

        return new JobBuilder("complexQsProteomeImport")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(importStep)
                .build();
    }

    private StepBuilder basicStepBuilder(
            String stepName,
            PlatformTransactionManager transactionManager,
            JobRepositoryFactoryBean jobRepository) throws Exception {

        return new StepBuilder(stepName)
                .transactionManager(transactionManager)
                .repository(jobRepository.getObject())
                .startLimit(5);
    }
}
