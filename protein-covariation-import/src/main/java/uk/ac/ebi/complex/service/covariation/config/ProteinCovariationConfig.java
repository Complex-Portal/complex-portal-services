package uk.ac.ebi.complex.service.covariation.config;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import psidev.psi.mi.jami.batch.BasicChunkLoggerListener;
import psidev.psi.mi.jami.batch.SimpleJobListener;
import uk.ac.ebi.complex.service.batch.config.AppProperties;
import uk.ac.ebi.complex.service.covariation.model.ProteinCovariation;
import uk.ac.ebi.complex.service.covariation.model.ProteinPairCovariation;
import uk.ac.ebi.complex.service.covariation.partitioner.ProteinCovariationPartitioner;
import uk.ac.ebi.complex.service.covariation.processor.ProteinCovariationPartitionProcessor;
import uk.ac.ebi.complex.service.covariation.processor.ProteinCovariationPreProcessTasklet;
import uk.ac.ebi.complex.service.covariation.reader.ProteinCovariationPairBatchReader;
import uk.ac.ebi.complex.service.covariation.reader.ProteinCovariationPartitionReader;
import uk.ac.ebi.complex.service.covariation.reader.ProteinIdsReader;
import uk.ac.ebi.complex.service.batch.service.UniProtMappingService;
import uk.ac.ebi.complex.service.covariation.writer.ProteinCovariationPairBatchWriter;
import uk.ac.ebi.complex.service.covariation.writer.ProteinCovariationPartitionWriter;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.service.ComplexService;
import uk.ac.ebi.intact.jami.service.ProteinPairCovariationService;

import java.util.List;

@Configuration
@EnableBatchProcessing
public class ProteinCovariationConfig {

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("/META-INF/protein-covariation-import.properties"));
        configurer.setIgnoreUnresolvablePlaceholders(true);
        return configurer;
    }

    @Bean
    @StepScope
    public ProteinCovariationPartitionReader proteinCovariationPartitionReader(
            CovariationFileConfiguration fileConfiguration,
            @Value("#{stepExecutionContext[startLine]}") int startLine,
            @Value("#{stepExecutionContext[partitionSize]}") int partitionSize,
            @Value("#{stepExecutionContext[partitionIndex]}") int partitionIndex) {

        return ProteinCovariationPartitionReader.builder()
                .fileConfiguration(fileConfiguration)
                .startLine(startLine)
                .partitionSize(partitionSize)
                .partitionIndex(partitionIndex)
                .build();
    }

    @Bean
    public ProteinCovariationPairBatchReader proteinCovariationPairBatchReader(CovariationFileConfiguration fileConfiguration) {
        return new ProteinCovariationPairBatchReader(fileConfiguration);
    }

    @Bean
    @StepScope
    public ProteinCovariationPartitionProcessor proteinCovariationPartitionProcessor(
            CovariationFileConfiguration fileConfiguration,
            @Value("#{stepExecutionContext[partitionIndex]}") int partitionIndex) {

        return ProteinCovariationPartitionProcessor.builder()
                .fileConfiguration(fileConfiguration)
                .partitionIndex(partitionIndex)
                .build();
    }

    @Bean
    @StepScope
    public ProteinCovariationPartitionWriter proteinCovariationPartitionWriter(
            CovariationFileConfiguration fileConfiguration,
            @Value("#{stepExecutionContext[partitionIndex]}") int partitionIndex) {

        return ProteinCovariationPartitionWriter.builder()
                .fileConfiguration(fileConfiguration)
                .partitionIndex(partitionIndex)
                .build();
    }

    @Bean
    public ProteinCovariationPairBatchWriter proteinCovariationPairBatchWriter(
            IntactDao intactDao,
            AppProperties appProperties,
            CovariationFileConfiguration fileConfiguration,
            ProteinPairCovariationService proteinPairCovariationService,
            @Value("${database.id}") String databaseId) {

        return ProteinCovariationPairBatchWriter.builder()
                .appProperties(appProperties)
                .fileConfiguration(fileConfiguration)
                .intactService(proteinPairCovariationService)
                .intactDao(intactDao)
                .databaseId(databaseId)
                .build();
    }

    @Bean
    public ProteinCovariationPreProcessTasklet proteinCovariationPreProcessTasklet(
            ComplexService complexService,
            UniProtMappingService uniProtMappingService,
            CovariationFileConfiguration fileConfiguration) {

        return new ProteinCovariationPreProcessTasklet(
                complexService,
                uniProtMappingService,
                new ProteinIdsReader(fileConfiguration),
                fileConfiguration);
    }

    @Bean
    public Step preProcessCovariationFile(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            ProteinCovariationPreProcessTasklet proteinCovariationPreProcessTasklet) throws Exception {

        return basicStepBuilder("preProcessCovariationFile", jamiTransactionManager, basicBatchJobRepository)
                .tasklet(proteinCovariationPreProcessTasklet)
                .build();
    }

    @Bean
    public Step processProteinCovariationFileStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ProteinCovariationPartitionReader proteinCovariationPartitionReader,
            ProteinCovariationPartitionProcessor proteinCovariationPartitionProcessor,
            ProteinCovariationPartitionWriter proteinCovariationPartitionWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "processProteinCovariationFileStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<ProteinCovariation, List<ProteinPairCovariation>>(basicStep)
                .chunk(250)
                .reader(proteinCovariationPartitionReader)
                .processor(proteinCovariationPartitionProcessor)
                .writer(proteinCovariationPartitionWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    @StepScope
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(25);
        threadPoolTaskExecutor.setMaxPoolSize(25);
        return threadPoolTaskExecutor;
    }

    @Bean
    public Step processProteinCovariationFilePartitionStep(
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ProteinCovariationPartitioner proteinCovariationPartitioner,
            ThreadPoolTaskExecutor threadPoolTaskExecutor,
            @Qualifier("processProteinCovariationFileStep") Step processProteinCovariationFileStep) throws Exception {

        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setStep(processProteinCovariationFileStep);
        partitionHandler.setGridSize(25);
        partitionHandler.setTaskExecutor(threadPoolTaskExecutor);

        return new StepBuilder("processProteinCovariationFilePartitionStep")
                .repository(basicBatchJobRepository.getObject())
                .partitioner("processProteinCovariationFileStep", proteinCovariationPartitioner)
                .partitionHandler(partitionHandler)
                .listener(basicChunkLoggerListener)
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
                .chunk(100)
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
            @Qualifier("preProcessCovariationFile") Step preProcessCovariationFile,
            @Qualifier("processProteinCovariationFilePartitionStep") Step processProteinCovariationFilePartitionStep) throws Exception {

        return new JobBuilder("processProteinCovariationFileJob")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(preProcessCovariationFile)
                .next(processProteinCovariationFilePartitionStep)
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

    @Bean
    public Job processAndImportProteinCovariationsJob(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobListener basicJobLoggerListener,
            @Qualifier("preProcessCovariationFile") Step preProcessCovariationFile,
            @Qualifier("processProteinCovariationFilePartitionStep") Step processProteinCovariationFilePartitionStep,
            @Qualifier("importProteinCovariationsStep") Step importProteinCovariationsStep) throws Exception {

        return new JobBuilder("processAndImportProteinCovariationsJob")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(preProcessCovariationFile)
                .next(processProteinCovariationFilePartitionStep)
                .next(importProteinCovariationsStep)
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
