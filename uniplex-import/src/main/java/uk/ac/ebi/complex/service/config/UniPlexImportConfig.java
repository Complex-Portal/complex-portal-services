package uk.ac.ebi.complex.service.config;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.support.PersistenceAnnotationBeanPostProcessor;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import psidev.psi.mi.jami.batch.BasicChunkLoggerListener;
import psidev.psi.mi.jami.batch.MIBatchJobManager;
import psidev.psi.mi.jami.batch.SimpleJobListener;
import psidev.psi.mi.jami.batch.SimpleJobRegistry;
import psidev.psi.mi.jami.bridges.uniprot.UniprotProteinFetcher;
import uk.ac.ebi.complex.service.ComplexFinder;
import uk.ac.ebi.complex.service.model.UniplexCluster;
import uk.ac.ebi.complex.service.model.UniplexComplex;
import uk.ac.ebi.complex.service.processor.UniplexClusterProcessor;
import uk.ac.ebi.complex.service.processor.UniplexFileProcessorTasklet;
import uk.ac.ebi.complex.service.reader.UniplexFileReader;
import uk.ac.ebi.complex.service.writer.UniplexComplexWriter;
import uk.ac.ebi.intact.jami.context.IntactConfiguration;
import uk.ac.ebi.intact.jami.context.UserContext;
import uk.ac.ebi.intact.jami.dao.IntactDao;
import uk.ac.ebi.intact.jami.model.extension.IntactSource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.Map;

@Configuration
public class UniPlexImportConfig {

    @Bean
    public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer configurer = new PropertyPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("/META-INF/uniplex-import.properties"));
        return configurer;
    }

    @Bean
    public JobRepositoryFactoryBean basicBatchJobRepository(
            DataSource jamiCoreDataSource,
            PlatformTransactionManager jamiTransactionManager) {

        JobRepositoryFactoryBean factoryBean = new JobRepositoryFactoryBean();
        factoryBean.setTablePrefix("ia_meta.BATCH_");
        factoryBean.setIsolationLevelForCreate("ISOLATION_DEFAULT");
        factoryBean.setDataSource(jamiCoreDataSource);
        factoryBean.setTransactionManager(jamiTransactionManager);
        return factoryBean;
    }

    @Bean
    public SimpleJobLauncher basicBatchJobLauncher(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SyncTaskExecutor syncTaskExecutor) throws Exception {

        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(basicBatchJobRepository.getObject());
        jobLauncher.setTaskExecutor(syncTaskExecutor);
        return jobLauncher;
    }

    @Bean
    public SyncTaskExecutor syncTaskExecutor() {
        return new SyncTaskExecutor();
    }

    @Bean
    public JobExplorerFactoryBean basicBatchJobExplorer(DataSource jamiCoreDataSource) {
        JobExplorerFactoryBean factoryBean = new JobExplorerFactoryBean();
        factoryBean.setTablePrefix("ia_meta.BATCH_");
        factoryBean.setDataSource(jamiCoreDataSource);
        return factoryBean;
    }

    @Bean
    public SimpleJobRegistry basicBatchJobRegistry() {
        return new SimpleJobRegistry();
    }

    @Bean
    public SimpleJobOperator basicBatchJobOperator(
            JobExplorerFactoryBean basicBatchJobExplorer,
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobRegistry basicBatchJobRegistry,
            SimpleJobLauncher basicBatchJobLauncher) throws Exception {

        SimpleJobOperator jobOperator = new SimpleJobOperator();
        jobOperator.setJobExplorer(basicBatchJobExplorer.getObject());
        jobOperator.setJobRepository(basicBatchJobRepository.getObject());
        jobOperator.setJobRegistry(basicBatchJobRegistry);
        jobOperator.setJobLauncher(basicBatchJobLauncher);
        return jobOperator;
    }

    @Bean
    public BasicChunkLoggerListener basicChunkLoggerListener() {
        return new BasicChunkLoggerListener();
    }

    @Bean
    public SimpleJobListener basicJobLoggerListener() {
        return new SimpleJobListener();
    }

    @Bean
    public MIBatchJobManager psiMIJobManager(
            JobRepositoryFactoryBean basicBatchJobRepository,
            SimpleJobOperator basicBatchJobOperator) throws Exception {

        MIBatchJobManager jobManager = new MIBatchJobManager();
        jobManager.setJobRepository(basicBatchJobRepository.getObject());
        jobManager.setJobOperator(basicBatchJobOperator);
        return jobManager;
    }

    @Bean
    public PersistenceExceptionTranslationPostProcessor persistenceExceptionTranslationPostProcessor() {
        return new PersistenceExceptionTranslationPostProcessor();
    }

    @Bean
    public PersistenceAnnotationBeanPostProcessor persistenceAnnotationBeanPostProcessor() {
        return new PersistenceAnnotationBeanPostProcessor();
    }

    @Bean
    public AutowiredAnnotationBeanPostProcessor autowiredAnnotationBeanPostProcessor() {
        return new AutowiredAnnotationBeanPostProcessor();
    }

    @Bean
    public JpaTransactionManager jamiTransactionManager(
            EntityManagerFactory intactEntityManagerFactory,
            DataSource jamiCoreDataSource) {

        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(intactEntityManagerFactory);
        transactionManager.setDataSource(jamiCoreDataSource);
        return transactionManager;
    }

    @Bean
    public FactoryBean<EntityManagerFactory> intactEntityManagerFactory(DataSource dataSource, AppProperties appProperties) {
        LocalContainerEntityManagerFactoryBean factoryBean = new LocalContainerEntityManagerFactoryBean();
        factoryBean.setPersistenceUnitName("intact-jami");
        factoryBean.setPersistenceXmlLocation("classpath*:/META-INF/jami-persistence.xml");
        factoryBean.setDataSource(dataSource);
        factoryBean.setJpaPropertyMap(Map.of(
                "hibernate.generate_statistics", true,
                "hibernate.format_sql", false,
                "hibernate.hbm2ddl.auto", appProperties.getHbm2ddl(),
                "hibernate.default_schema", "intact"
        ));
        HibernateJpaVendorAdapter jpaVendorAdapter = new HibernateJpaVendorAdapter();
        jpaVendorAdapter.setDatabasePlatform(appProperties.getDbDialect());
        jpaVendorAdapter.setShowSql(false);
        jpaVendorAdapter.setGenerateDdl(false);
        factoryBean.setJpaVendorAdapter(jpaVendorAdapter);
        return factoryBean;
    }

    @Bean
    public DataSource jamiCoreDataSource(AppProperties appProperties) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(appProperties.getDbDriver());
        dataSource.setUrl(appProperties.getDbUrl());
        dataSource.setUsername(appProperties.getDbUsername());
        dataSource.setPassword(appProperties.getDbPassword());
        return dataSource;
    }

    @Bean
    public UserContext jamiUserContext(AppProperties appProperties) {
        UserContext userContext = new UserContext();
        userContext.setUserId(appProperties.getUserContextId());
        return userContext;
    }

    @Bean
    public IntactConfiguration intactJamiConfiguration(IntactSource sourceIntact, AppProperties appProperties) {
        IntactConfiguration intactConfiguration = new IntactConfiguration();
        intactConfiguration.setAcPrefix(appProperties.getAcPrefix());
        intactConfiguration.setDefaultInstitution(sourceIntact);
        return intactConfiguration;
    }

    @Bean
    public IntactSource sourceIntact() {
        IntactSource source = new IntactSource("intact");
        source.setFullName("European Bioinformatics Institute");
        source.setMIIdentifier("MI:0469");
        source.setUrl("http://www.ebi.ac.uk/intact/");
        source.setPostalAddress("European Bioinformatics Institute; Wellcome Trust Genome Campus; Hinxton, Cambridge; CB10 1SD; United Kingdom");
        return source;
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
    public Step uniplexClusterImportStep(
            PlatformTransactionManager jamiTransactionManager,
            JobRepositoryFactoryBean basicBatchJobRepository,
            BasicChunkLoggerListener basicChunkLoggerListener,
            UniplexFileReader uniplexFileReader,
            UniplexClusterProcessor uniplexClusterProcessor,
            UniplexComplexWriter uniplexComplexWriter) throws Exception {

        StepBuilder basicStep = basicStepBuilder(
                "uniplexClusterImportStep",
                jamiTransactionManager,
                basicBatchJobRepository);

        return new SimpleStepBuilder<UniplexCluster, UniplexComplex>(basicStep)
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
            @Qualifier("uniplexClusterImportStep") Step importStep) throws Exception {

        return new JobBuilder("uniplexClusterImport")
                .repository(basicBatchJobRepository.getObject())
                .listener(basicJobLoggerListener)
                .start(processUniplexFile)
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
