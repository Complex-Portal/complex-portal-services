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
import uk.ac.ebi.complex.service.ComplexFinder;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblies;
import uk.ac.ebi.complex.service.model.ComplexWithAssemblyXrefs;
import uk.ac.ebi.complex.service.processor.PdbAssembliesProcessor;
import uk.ac.ebi.complex.service.processor.PdbFileProcessorTasklet;
import uk.ac.ebi.complex.service.reader.PdbAssembliesFileReader;
import uk.ac.ebi.complex.service.reader.PdbAssembliesReader;
import uk.ac.ebi.complex.service.writer.PdbAssembliesWriter;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.service.ComplexService;

@Configuration
public class PdbImportConfig {

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("/META-INF/pdb-import.properties"));
        return configurer;
    }

    @Bean
    public ComplexFinder complexFinder(IntactDao intactDao) {
        return new ComplexFinder(intactDao);
    }

    @Bean
    public PdbAssembliesProcessor pdbAssembliesProcessor(
            IntactDao intactDao,
            PdbAssembliesFileReader pdbAssembliesFileReader,
            FileConfiguration fileConfiguration) {

        return PdbAssembliesProcessor.builder()
                .intactDao(intactDao)
                .pdbAssembliesFileReader(pdbAssembliesFileReader)
                .fileConfiguration(fileConfiguration)
                .build();
    }

    @Bean
    public PdbAssembliesWriter pdbAssembliesWriter(
            IntactDao intactDao,
            AppProperties appProperties,
            FileConfiguration fileConfiguration,
            ComplexService complexService) {

        return PdbAssembliesWriter.builder()
                .intactDao(intactDao)
                .appProperties(appProperties)
                .fileConfiguration(fileConfiguration)
                .complexService(complexService)
                .build();

    }

    @Bean
    public Step pdbAssembliesImportStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            PdbAssembliesReader pdbAssembliesReader,
            PdbAssembliesProcessor pdbAssembliesProcessor,
            PdbAssembliesWriter pdbAssembliesWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "pdbAssembliesImportStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<ComplexWithAssemblies, ComplexWithAssemblyXrefs>(basicStep)
                .chunk(50)
                .reader(pdbAssembliesReader)
                .processor(pdbAssembliesProcessor)
                .writer(pdbAssembliesWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    public Step processPdbFile(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            PdbFileProcessorTasklet pdbFileProcessorTasklet) throws Exception {

        return basicStepBuilder("processPdbFile", jamiTransactionManager, basicBatchJobRepository)
                .tasklet(pdbFileProcessorTasklet)
                .build();
    }

    @Bean
    public Job pdbAssembliesImport(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobListener basicJobLoggerListener,
            @Qualifier("processPdbFile") Step processPdbFile,
            @Qualifier("pdbAssembliesImportStep") Step importStep) throws Exception {

        return new JobBuilder("pdbAssembliesImport")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(processPdbFile)
                .next(importStep)
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
