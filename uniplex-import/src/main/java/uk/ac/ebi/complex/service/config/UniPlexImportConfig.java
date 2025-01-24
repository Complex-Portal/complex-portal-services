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
import psidev.psi.mi.jami.bridges.uniprot.UniprotProteinFetcher;
import uk.ac.ebi.complex.service.ComplexFinder;
import uk.ac.ebi.complex.service.manager.UniplexComplexManager;
import uk.ac.ebi.complex.service.model.ComplexWithMatches;
import uk.ac.ebi.complex.service.model.ComplexWithXrefsToDelete;
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.complex.service.processor.ComplexImportBatchProcessor;
import uk.ac.ebi.complex.service.processor.ComplexXrefDeleteProcessor;
import uk.ac.ebi.complex.service.processor.UniplexFileProcessorTasklet;
import uk.ac.ebi.complex.service.reader.ComplexImportBatchReader;
import uk.ac.ebi.complex.service.reader.ComplexIteratorBatchReader;
import uk.ac.ebi.complex.service.reader.UniplexClusterReader;
import uk.ac.ebi.complex.service.writer.ComplexImportBatchWriter;
import uk.ac.ebi.complex.service.writer.ComplexXrefDeleteWriter;
import uk.ac.ebi.complex.service.writer.UniplexClusterWriter;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactComplex;
import uk.ac.ebi.intact.jami.service.ComplexService;

@Configuration
public class UniPlexImportConfig {

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("/META-INF/uniplex-import.properties"));
        return configurer;
    }

    @Bean
    public ComplexFinder complexFinder(IntactDao intactDao) {
        return new ComplexFinder(intactDao);
    }

    @Bean
    public UniprotProteinFetcher uniprotProteinFetcher() {
        return new UniprotProteinFetcher();
    }

    @Bean
    public UniplexComplexManager uniplexComplexManager(
            IntactDao intactDao,
            UniprotProteinFetcher uniprotProteinFetcher,
            AppProperties appProperties) {

        return UniplexComplexManager.builder()
                .intactDao(intactDao)
                .uniprotProteinFetcher(uniprotProteinFetcher)
                .appProperties(appProperties)
                .build();
    }

    @Bean
    public ComplexImportBatchProcessor<Integer, UniplexCluster> uniplexBatchProcessor(
            ComplexFinder complexFinder,
            UniplexComplexManager uniplexComplexManager,
            FileConfiguration fileConfiguration) {

        return ComplexImportBatchProcessor.<Integer, UniplexCluster>builder()
                .complexFinder(complexFinder)
                .complexManager(uniplexComplexManager)
                .fileConfiguration(fileConfiguration)
                .build();
    }

    @Bean
    public UniplexClusterReader uniplexClusterReader(FileConfiguration fileConfiguration) {
        return UniplexClusterReader.builder().fileConfiguration(fileConfiguration).build();
    }

    @Bean
    public ComplexImportBatchReader<Integer, UniplexCluster> uniplexBatchReader(
            FileConfiguration fileConfiguration,
            UniplexClusterReader uniplexClusterReader) {

        return new ComplexImportBatchReader<>(fileConfiguration, uniplexClusterReader);
    }

    @Bean
    public UniplexClusterWriter uniplexClusterWriter(FileConfiguration fileConfiguration) {
        return UniplexClusterWriter.builder().fileConfiguration(fileConfiguration).build();
    }

    @Bean
    public ComplexImportBatchWriter<Integer, UniplexCluster> uniplexBatchWriter(
            UniplexComplexManager uniplexComplexManager,
            ComplexService complexService,
            FileConfiguration fileConfiguration,
            AppProperties appProperties) {

        return ComplexImportBatchWriter.<Integer, UniplexCluster>builder()
                .complexManager(uniplexComplexManager)
                .complexService(complexService)
                .fileConfiguration(fileConfiguration)
                .appProperties(appProperties)
                .build();
    }

    @Bean
    public Step uniplexClusterImportStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ComplexImportBatchReader<Integer, UniplexCluster> uniplexFileReader,
            ComplexImportBatchProcessor<Integer, UniplexCluster> uniplexClusterProcessor,
            ComplexImportBatchWriter<Integer, UniplexCluster> uniplexComplexWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "uniplexClusterImportStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<UniplexCluster, ComplexWithMatches<Integer, UniplexCluster>>(basicStep)
                .chunk(50)
                .reader(uniplexFileReader)
                .processor(uniplexClusterProcessor)
                .writer(uniplexComplexWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    public ComplexIteratorBatchReader complexIteratorBatchReader(ComplexService complexService) {
        return new ComplexIteratorBatchReader(complexService);
    }

    @Bean
    public ComplexXrefDeleteProcessor<Integer, UniplexCluster> clusterComplexXrefDeleteProcessor(
            UniplexComplexManager uniplexComplexManager,
            IntactDao intactDao,
            FileConfiguration fileConfiguration) {

        return ComplexXrefDeleteProcessor.<Integer, UniplexCluster>builder()
                .complexManager(uniplexComplexManager)
                .intactDao(intactDao)
                .fileConfiguration(fileConfiguration)
                .databaseId(UniplexComplexManager.HUMAP_DATABASE_ID)
                .build();
    }

    @Bean
    public ComplexXrefDeleteWriter complexXrefDeleteWriter(
            ComplexService complexService,
            FileConfiguration fileConfiguration) {

        return ComplexXrefDeleteWriter.builder()
                .complexService(complexService)
                .fileConfiguration(fileConfiguration)
                .build();
    }

    @Bean
    public Step deleteOldXrefsStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ComplexIteratorBatchReader complexIteratorBatchReader,
            ComplexXrefDeleteProcessor<Integer, UniplexCluster> clusterComplexXrefDeleteProcessor,
            ComplexXrefDeleteWriter complexXrefDeleteWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "deleteOldXrefsStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<IntactComplex, ComplexWithXrefsToDelete>(basicStep)
                .chunk(50)
                .reader(complexIteratorBatchReader)
                .processor(clusterComplexXrefDeleteProcessor)
                .writer(complexXrefDeleteWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    public Step processUniplexFile(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            UniplexFileProcessorTasklet uniplexFileProcessorTasklet) throws Exception {

        return basicStepBuilder("processUniplexFile", jamiTransactionManager, basicBatchJobRepository)
                .tasklet(uniplexFileProcessorTasklet)
                .build();
    }

    @Bean
    public Job uniplexClusterImport(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobListener basicJobLoggerListener,
            @Qualifier("processUniplexFile") Step processUniplexFile,
            @Qualifier("uniplexClusterImportStep") Step importStep,
            @Qualifier("deleteOldXrefsStep") Step deleteOldXrefsStep) throws Exception {

        return new JobBuilder("uniplexClusterImport")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(processUniplexFile)
                .next(importStep)
                .next(deleteOldXrefsStep)
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
