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
import uk.ac.ebi.complex.service.manager.MusicComplexManager;
import uk.ac.ebi.complex.service.model.ComplexWithMatches;
import uk.ac.ebi.complex.service.model.MusicComplexToImport;
import uk.ac.ebi.complex.service.processor.ComplexImportBatchProcessor;
import uk.ac.ebi.complex.service.processor.MusicFileProcessorTasklet;
import uk.ac.ebi.complex.service.reader.ComplexImportBatchReader;
import uk.ac.ebi.complex.service.reader.MusicComplexReader;
import uk.ac.ebi.complex.service.writer.ComplexImportBatchWriter;
import uk.ac.ebi.complex.service.writer.MusicComplexWriter;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.service.ComplexService;

@Configuration
public class MusicImportConfig {

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("/META-INF/music-import.properties"));
        configurer.setIgnoreUnresolvablePlaceholders(true);
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
    public MusicComplexManager musicComplexManager(
            IntactDao intactDao,
            UniprotProteinFetcher uniprotProteinFetcher,
            AppProperties appProperties,
            MusicImportAppProperties musicImportAppProperties) {

        return MusicComplexManager.builder()
                .intactDao(intactDao)
                .uniprotProteinFetcher(uniprotProteinFetcher)
                .appProperties(appProperties)
                .musicImportAppProperties(musicImportAppProperties)
                .build();
    }

    @Bean
    public ComplexImportBatchProcessor<Double, MusicComplexToImport> musicBatchProcessor(
            ComplexFinder complexFinder,
            MusicComplexManager musicComplexManager,
            FileConfiguration fileConfiguration) {

        return ComplexImportBatchProcessor.<Double, MusicComplexToImport>builder()
                .complexFinder(complexFinder)
                .complexManager(musicComplexManager)
                .fileConfiguration(fileConfiguration)
                .build();
    }

    @Bean
    public MusicComplexReader musicComplexReader(
            FileConfiguration fileConfiguration,
            MusicImportAppProperties musicImportAppProperties) {

        return MusicComplexReader.builder()
                .fileConfiguration(fileConfiguration)
                .musicImportAppProperties(musicImportAppProperties)
                .build();
    }

    @Bean
    public ComplexImportBatchReader<Double, MusicComplexToImport> musicBatchReader(
            FileConfiguration fileConfiguration,
            MusicComplexReader musicComplexReader) {

        return new ComplexImportBatchReader<>(fileConfiguration, musicComplexReader);
    }

    @Bean
    public MusicComplexWriter musicComplexWriter(
            FileConfiguration fileConfiguration,
            MusicImportAppProperties musicImportAppProperties) {

        return MusicComplexWriter.builder()
                .fileConfiguration(fileConfiguration)
                .musicImportAppProperties(musicImportAppProperties)
                .build();
    }

    @Bean
    public ComplexImportBatchWriter<Double, MusicComplexToImport> musicBatchWriter(
            MusicComplexManager musicComplexManager,
            ComplexService complexService,
            FileConfiguration fileConfiguration,
            AppProperties appProperties) {

        return ComplexImportBatchWriter.<Double, MusicComplexToImport>builder()
                .complexManager(musicComplexManager)
                .intactService(complexService)
                .fileConfiguration(fileConfiguration)
                .appProperties(appProperties)
                .build();
    }

    @Bean
    public Step musicComplexesImportStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            ComplexImportBatchReader<Double, MusicComplexToImport> musicBatchReader,
            ComplexImportBatchProcessor<Double, MusicComplexToImport> musicBatchProcessor,
            ComplexImportBatchWriter<Double, MusicComplexToImport> musicBatchWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "musicComplexesImportStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<MusicComplexToImport, ComplexWithMatches<Double, MusicComplexToImport>>(basicStep)
                .chunk(50)
                .reader(musicBatchReader)
                .processor(musicBatchProcessor)
                .writer(musicBatchWriter)
                .faultTolerant()
                .retryLimit(10)
                .retry(org.springframework.batch.item.ItemStreamException.class)
                .retry(javax.net.ssl.SSLHandshakeException.class)
                .listener((StepExecutionListener) basicChunkLoggerListener)
                .listener((ChunkListener) basicChunkLoggerListener)
                .build();
    }

    @Bean
    public Step processMusicFile(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            MusicFileProcessorTasklet musicFileProcessorTasklet) throws Exception {

        return basicStepBuilder("processMusicFile", jamiTransactionManager, basicBatchJobRepository)
                .tasklet(musicFileProcessorTasklet)
                .build();
    }

    @Bean
    public Job musicComplexesImport(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobListener basicJobLoggerListener,
            @Qualifier("processMusicFile") Step processMusicFile,
            @Qualifier("musicComplexesImportStep") Step importStep) throws Exception {

        return new JobBuilder("musicComplexesImport")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(processMusicFile)
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
