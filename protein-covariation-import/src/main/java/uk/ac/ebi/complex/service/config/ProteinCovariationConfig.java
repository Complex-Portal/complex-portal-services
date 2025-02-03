package uk.ac.ebi.complex.service.config;

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
import uk.ac.ebi.complex.service.model.ProteinCovariation;
import uk.ac.ebi.complex.service.model.ProteinPairCovariation;
import uk.ac.ebi.complex.service.processor.ProteinCovariationBatchProcessor;
import uk.ac.ebi.complex.service.reader.ProteinCovariationBatchReader;
import uk.ac.ebi.complex.service.reader.ProteinCovariationPairBatchReader;
import uk.ac.ebi.complex.service.reader.ProteinIdsReader;
import uk.ac.ebi.complex.service.service.UniProtMappingService;
import uk.ac.ebi.complex.service.writer.ProteinCovariationBatchWriter;
import uk.ac.ebi.complex.service.writer.ProteinCovariationPairBatchWriter;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.service.ComplexService;
import uk.ac.ebi.intact.jami.service.ProteinPairCovariationService;

import java.util.List;

@Configuration
public class ProteinCovariationConfig {

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("/META-INF/protein-covariation-import.properties"));
        configurer.setIgnoreUnresolvablePlaceholders(true);
        return configurer;
    }

    @Bean
    public ProteinCovariationBatchReader proteinCovariationBatchReader(FileConfiguration fileConfiguration) {
        return new ProteinCovariationBatchReader(fileConfiguration);
    }

    @Bean
    public ProteinCovariationPairBatchReader proteinCovariationPairBatchReader(FileConfiguration fileConfiguration) {
        return new ProteinCovariationPairBatchReader(fileConfiguration);
    }

    @Bean
    public ProteinCovariationBatchProcessor proteinCovariationBatchProcessor(
            FileConfiguration fileConfiguration,
            IntactDao intactDao,
            UniProtMappingService uniProtMappingService,
            ComplexService complexService) {

        return ProteinCovariationBatchProcessor.builder()
                .fileConfiguration(fileConfiguration)
                .intactDao(intactDao)
                .uniProtMappingService(uniProtMappingService)
                .proteinIdsReader(new ProteinIdsReader(fileConfiguration))
                .complexService(complexService)
                .build();
    }

    @Bean
    public ProteinCovariationBatchWriter proteinCovariationBatchWriter(FileConfiguration fileConfiguration) {
        return ProteinCovariationBatchWriter.builder()
                .fileConfiguration(fileConfiguration)
                .build();
    }

    @Bean
    public ProteinCovariationPairBatchWriter proteinCovariationPairBatchWriter(
            IntactDao intactDao,
            AppProperties appProperties,
            FileConfiguration fileConfiguration,
            ProteinPairCovariationService proteinPairCovariationService) {

        return ProteinCovariationPairBatchWriter.builder()
                .appProperties(appProperties)
                .fileConfiguration(fileConfiguration)
                .intactService(proteinPairCovariationService)
                .intactDao(intactDao)
                .build();
    }

    @Bean
    public Step processProteinCovariationFileStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ProteinCovariationBatchReader proteinCovariationBatchReader,
            ProteinCovariationBatchProcessor proteinCovariationBatchProcessor,
            ProteinCovariationBatchWriter proteinCovariationBatchWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "processProteinCovariationFileStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<List<ProteinCovariation>, List<ProteinPairCovariation>>(basicStep)
                .chunk(50)
                .reader(proteinCovariationBatchReader)
                .processor(proteinCovariationBatchProcessor)
                .writer(proteinCovariationBatchWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    public Step importProteinCovariationsStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ProteinCovariationPairBatchReader proteinCovariationPairBatchReader,
            ProteinCovariationPairBatchWriter proteinCovariationPairBatchWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "importProteinCovariationsStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<List<ProteinPairCovariation>, List<ProteinPairCovariation>>(basicStep)
                .chunk(50)
                .reader(proteinCovariationPairBatchReader)
                .writer(proteinCovariationPairBatchWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    public Job processProteinCovariationFileJob(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobListener basicJobLoggerListener,
            @Qualifier("processProteinCovariationFileStep") Step processProteinCovariationFileStep) throws Exception {

        return new JobBuilder("processProteinCovariationFileJob")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(processProteinCovariationFileStep)
                .build();
    }

    @Bean
    public Job importProteinCovariationsJob(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobListener basicJobLoggerListener,
            @Qualifier("importProteinCovariationsStep") Step importProteinCovariationsStep) throws Exception {

        return new JobBuilder("importProteinCovariationsJob")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(importProteinCovariationsStep)
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
